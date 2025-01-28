/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.service;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Collection;
import java.util.Optional;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.logsafe.SafeArg;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.net.IAsyncCallbackWithFailure;
import org.apache.cassandra.schema.LegacySchemaTables;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.WrappedRunnable;


class MigrationTask extends WrappedRunnable
{
    private static final Logger logger = LoggerFactory.getLogger(MigrationTask.class);

    private final InetAddress endpoint;
    private final Optional<UUID> version;

    MigrationTask(InetAddress endpoint)
    {
        this.endpoint = endpoint;
        this.version = Optional.empty();
    }

    MigrationTask(InetAddress endpoint, UUID version)
    {
        this.endpoint = endpoint;
        this.version = Optional.of(version);
    }

    public void runMayThrow() throws Exception
    {
        if (!FailureDetector.instance.isAlive(endpoint))
        {
            logger.warn("Can't send schema pull request: node {} is down.", SafeArg.of("endpoint", endpoint));
            version.ifPresent(v -> MigrationManager.removeEndpointFromSchemaPullVersion(v, endpoint));
            return;
        }

        // There is a chance that quite some time could have passed between now and the MM#maybeScheduleSchemaPull(),
        // potentially enough for the endpoint node to restart - which is an issue if it does restart upgraded, with
        // a higher major.
        if (!MigrationManager.shouldPullSchemaFrom(endpoint))
        {
            logger.info("Skipped sending a migration request: node {} has a higher major version now.", SafeArg.of("endpoint", endpoint));
            version.ifPresent(v -> MigrationManager.removeEndpointFromSchemaPullVersion(v, endpoint));
            return;
        }

        MessageOut message = new MessageOut<>(MessagingService.Verb.MIGRATION_REQUEST, null, MigrationManager.MigrationsSerializer.instance);

        IAsyncCallbackWithFailure<Collection<Mutation>> cb = new IAsyncCallbackWithFailure<Collection<Mutation>>()
        {
            @Override
            public void response(MessageIn<Collection<Mutation>> message)
            {
                try
                {
                    logger.debug("Processing response to schema pull from endpoint", SafeArg.of("endpoint", endpoint));
                    LegacySchemaTables.mergeSchema(message.payload);
                }
                catch (IOException e)
                {
                    logger.error("IOException merging remote schema", e);
                }
                catch (ConfigurationException e)
                {
                    logger.error("Configuration exception merging remote schema", e);
                }
                finally
                {
                    // always attempt to clean up our outstanding schema pull request if created with a version
                    version.ifPresent(v -> {
                        logger.debug("Successfully processed response to schema pull",
                                     SafeArg.of("endpoint", endpoint),
                                     SafeArg.of("schemaVersion", v));
                        MigrationManager.removeEndpointFromSchemaPullVersion(v, endpoint);
                    });
                }
            }

            @Override
            public void onFailure(InetAddress from)
            {
                // always attempt to clean up our outstanding schema pull request if created with a version
                version.ifPresent(v -> {
                    logger.debug("Timed out waiting for response to schema pull",
                                 SafeArg.of("endpoint", endpoint),
                                 SafeArg.of("schemaVersion", v));
                    MigrationManager.removeEndpointFromSchemaPullVersion(v, endpoint);
                });
            }

            public boolean isLatencyForSnitch()
            {
                return false;
            }
        };
        try {
            MessagingService.instance().sendRRWithFailure(message, endpoint, cb);
        }
        catch (Exception e)
        {
            version.ifPresent(v -> MigrationManager.removeEndpointFromSchemaPullVersion(v, endpoint));
            throw e;
        }
   }
}
