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
package org.apache.cassandra.db;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;

import com.palantir.cassandra.utils.OwnershipVerificationUtils;
import org.apache.cassandra.exceptions.IsBootstrappingException;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tracing.Tracing;

public class ReadVerbHandler implements IVerbHandler<ReadCommand>
{
    public void doVerb(MessageIn<ReadCommand> message, int id)
    {
        if (StorageService.instance.isBootstrapMode())
        {
            /* Don't service reads! */
            throw new IsBootstrappingException();
        }

        ReadCommand command = message.payload;
        OwnershipVerificationUtils.verifyRead(command);
        Keyspace keyspace = Keyspace.open(command.ksName);
        Row row = command.getRow(keyspace);

        MessageOut<ReadResponse> reply = new MessageOut<ReadResponse>(MessagingService.Verb.REQUEST_RESPONSE,
                                                                      getResponse(command, row),
                                                                      ReadResponse.serializer);

        if (command.isDigestQuery())
        {
            Keyspace.open(command.ksName).getColumnFamilyStore(command.cfName).metric.digestReads.mark();
        }
        else
        {
            Keyspace.open(command.ksName).getColumnFamilyStore(command.cfName).metric.dataReads.mark();
        }

        Tracing.trace("Enqueuing response to {}", message.from);
        MessagingService.instance().sendReply(reply, id, message.from);
    }

    public static ReadResponse getResponse(ReadCommand command, Row row)
    {
        if (command.isDigestQuery())
        {
            return new ReadResponse(ColumnFamily.digest(row.cf), (row.cf == null || row.cf.pageToken() == null) ? null : row.cf.pageToken().digest());
        }
        else
        {
            return new ReadResponse(row);
        }
    }
}
