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

package com.palantir.cassandra.utils;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.logsafe.SafeArg;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

public class SchemaAgreementCheck
{
    private static final Logger logger = LoggerFactory.getLogger(SchemaAgreementCheck.class);
    private final Supplier<UUID> localSchemaVersionSupplier;
    private final Supplier<Set<Map.Entry<InetAddress, EndpointState>>> endpointStatesSupplier;
    private final InetAddress localAddress;

    public SchemaAgreementCheck()
    {
        this(Schema.instance::getVersion, Gossiper.instance::getEndpointStates, FBUtilities.getBroadcastAddress());
    }

    @VisibleForTesting
    SchemaAgreementCheck(Supplier<UUID> localSchemaVersionSupplier, Supplier<Set<Map.Entry<InetAddress, EndpointState>>> endpointStatesSupplier, InetAddress localAddress)
    {
        this.localSchemaVersionSupplier = localSchemaVersionSupplier;
        this.endpointStatesSupplier = endpointStatesSupplier;
        this.localAddress = localAddress;
    }

    public boolean isSchemaInAgreement() {
        return isSchemaInAgreement(ImmutableList.of());
    }

    public boolean isSchemaInAgreement(List<InetAddress> ignoredEndpoints)
    {
        try
        {
            UUID localSchemaVersion = localSchemaVersionSupplier.get();
            Set<Map.Entry<InetAddress, EndpointState>> endpointStates = endpointStatesSupplier.get();

            boolean peerEndpointsExist = endpointStates.stream()
                          .anyMatch(endpointState -> !localAddress.equals(endpointState.getKey()));
            boolean schemaIsInAgreeement = endpointStates.stream()
                          .filter(endpointState -> !ignoredEndpoints.contains(endpointState.getKey()))
                          .filter(endpointState -> !isLeftOrRemoved(endpointState.getValue()))
                          .allMatch(endpointState -> schemaIsEqualToLocalVersion(localSchemaVersion, endpointState.getKey(), endpointState.getValue()));
            return peerEndpointsExist && schemaIsInAgreeement;
        }
        catch (Exception e)
        {
            logger.error("Exception while checking schema agreement", e);
            return false;
        }
    }

    private boolean isLeftOrRemoved(EndpointState endpointState)
    {
        return VersionedValue.STATUS_LEFT.equals(endpointState.getStatus())
               || VersionedValue.REMOVED_TOKEN.equals(endpointState.getStatus());
    }

    private boolean schemaIsEqualToLocalVersion(UUID localSchemaVersion, InetAddress address, EndpointState endpointState)
    {
        VersionedValue schema = endpointState.getApplicationState(ApplicationState.SCHEMA);
        boolean match = schema != null && localSchemaVersion.equals(UUID.fromString(schema.value));
        if(!match) {
            logger.warn("Schema agreement check failure",
                        SafeArg.of("localSchemaVersion", localSchemaVersion),
                        SafeArg.of("endpoint", address),
                        SafeArg.of("remoteSchemaVersion", schema));
        }
        return match;
    }
}
