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

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;
import com.google.common.net.InetAddresses;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.EndpointStateFactory;
import org.apache.cassandra.gms.VersionedValue;

import static org.assertj.core.api.Assertions.assertThat;

public class SchemaAgreementCheckTest
{
    private static final VersionedValue.VersionedValueFactory valueFactory = new VersionedValue.VersionedValueFactory(DatabaseDescriptor.getPartitioner());

    @Test
    public void checkSchemaAgreement_passes()
    {
        UUID schema = UUID.randomUUID();
        EndpointState state = createNormal(schema);

        SchemaAgreementCheck schemaAgreementCheck = new SchemaAgreementCheck(() -> schema,
                                                                             () -> ImmutableMap.of(InetAddresses.forString("127.0.0.1"), state,
                                                                                                   InetAddresses.forString("127.0.0.2"), state,
                                                                                                   InetAddresses.forString("127.0.0.3"), state).entrySet());
        assertThat(schemaAgreementCheck.isSchemaInAgreement()).isTrue();
    }

    @Test
    public void checkSchemaAgreement_failsOnNullSchema()
    {
        UUID schema = UUID.randomUUID();
        EndpointState state1 = createNormal(schema);

        EndpointState state2 = EndpointStateFactory.create();
        List<Token> tokens = Collections.singletonList(DatabaseDescriptor.getPartitioner().getRandomToken());
        state2.addApplicationState(ApplicationState.STATUS, valueFactory.normal(tokens));
        state2.addApplicationState(ApplicationState.TOKENS, valueFactory.tokens(tokens));

        SchemaAgreementCheck schemaAgreementCheck = new SchemaAgreementCheck(() -> schema,
                                                                             () -> ImmutableMap.of(InetAddresses.forString("127.0.0.1"), state1,
                                                                                                   InetAddresses.forString("127.0.0.2"), state1,
                                                                                                   InetAddresses.forString("127.0.0.3"), state2).entrySet());
        assertThat(schemaAgreementCheck.isSchemaInAgreement()).isFalse();
    }

    @Test
    public void checkSchemaAgreement_failsOnIncorrectSchema()
    {
        UUID schema1 = UUID.randomUUID();
        EndpointState state1 = createNormal(schema1);
        EndpointState state2 = createNormal(UUID.randomUUID());

        SchemaAgreementCheck schemaAgreementCheck = new SchemaAgreementCheck(() -> schema1,
                                                                             () -> ImmutableMap.of(InetAddresses.forString("127.0.0.1"), state1,
                                                                                                   InetAddresses.forString("127.0.0.2"), state1,
                                                                                                   InetAddresses.forString("127.0.0.3"), state2).entrySet());
        assertThat(schemaAgreementCheck.isSchemaInAgreement()).isFalse();
    }

    @Test
    public void checkSchemaAgreement_ignoresLeftNode()
    {
        UUID schema1 = UUID.randomUUID();
        UUID schema2 = UUID.randomUUID();
        EndpointState state1 = createNormal(schema1);
        EndpointState state2 = createLeft(schema2);

        SchemaAgreementCheck schemaAgreementCheck = new SchemaAgreementCheck(() -> schema1,
                                                                             () -> ImmutableMap.of(InetAddresses.forString("127.0.0.1"), state1,
                                                                                                   InetAddresses.forString("127.0.0.2"), state1,
                                                                                                   InetAddresses.forString("127.0.0.3"), state2).entrySet());
        assertThat(schemaAgreementCheck.isSchemaInAgreement()).isTrue();
    }

    @Test
    public void checkSchemaAgreement_doesNotIgnoreNullStatus()
    {
        UUID schema1 = UUID.randomUUID();
        UUID schema2 = UUID.randomUUID();
        EndpointState state1 = createNormal(schema1);

        EndpointState state2 = EndpointStateFactory.create();
        List<Token> tokens = Collections.singletonList(DatabaseDescriptor.getPartitioner().getRandomToken());
        state2.addApplicationState(ApplicationState.SCHEMA, valueFactory.schema(schema2));
        state2.addApplicationState(ApplicationState.TOKENS, valueFactory.tokens(tokens));

        SchemaAgreementCheck schemaAgreementCheck = new SchemaAgreementCheck(() -> schema1,
                                                                             () -> ImmutableMap.of(InetAddresses.forString("127.0.0.1"), state1,
                                                                                                   InetAddresses.forString("127.0.0.2"), state1,
                                                                                                   InetAddresses.forString("127.0.0.3"), state2).entrySet());
        assertThat(schemaAgreementCheck.isSchemaInAgreement()).isFalse();
    }

    private static EndpointState createNormal(UUID schema)
    {
        EndpointState state = EndpointStateFactory.create();
        List<Token> tokens = Collections.singletonList(DatabaseDescriptor.getPartitioner().getRandomToken());
        state.addApplicationState(ApplicationState.STATUS, valueFactory.normal(tokens));
        state.addApplicationState(ApplicationState.SCHEMA, valueFactory.schema(schema));
        state.addApplicationState(ApplicationState.TOKENS, valueFactory.tokens(tokens));
        return state;
    }

    private static EndpointState createLeft(UUID schema)
    {
        EndpointState state = EndpointStateFactory.create();
        List<Token> tokens = Collections.singletonList(DatabaseDescriptor.getPartitioner().getRandomToken());
        state.addApplicationState(ApplicationState.STATUS, valueFactory.left(tokens, 1000));
        state.addApplicationState(ApplicationState.SCHEMA, valueFactory.schema(schema));
        state.addApplicationState(ApplicationState.TOKENS, valueFactory.tokens(tokens));
        return state;
    }
}
