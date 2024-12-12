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
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.Schema;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;

public class SchemaAgreementCheck
{
    private final Supplier<UUID> localSchemaVersionSupplier;
    private final Supplier<Set<Map.Entry<InetAddress, EndpointState>>> endpointStatesSupplier;

    public SchemaAgreementCheck()
    {
        this(Schema.instance::getVersion, Gossiper.instance::getEndpointStates);
    }

    @VisibleForTesting
    SchemaAgreementCheck(Supplier<UUID> localSchemaVersionSupplier, Supplier<Set<Map.Entry<InetAddress, EndpointState>>> endpointStatesSupplier)
    {
        this.localSchemaVersionSupplier = localSchemaVersionSupplier;
        this.endpointStatesSupplier = endpointStatesSupplier;
    }

    public boolean isSchemaInAgreement()
    {
        UUID localSchemaVersion = localSchemaVersionSupplier.get();
        return endpointStatesSupplier.get().stream()
                                     .map(Map.Entry::getValue)
                                     .filter(value -> !isLeft(value))
                                     .allMatch(value -> schemaIsEqualToLocalVersion(localSchemaVersion, value));
    }

    private boolean isLeft(EndpointState endpointState)
    {
        return VersionedValue.STATUS_LEFT.equals(endpointState.getStatus());
    }

    private boolean schemaIsEqualToLocalVersion(UUID localSchemaVersion, EndpointState endpointState)
    {
        VersionedValue schema = endpointState.getApplicationState(ApplicationState.SCHEMA);
        return schema != null && localSchemaVersion.equals(UUID.fromString(schema.value));
    }
}
