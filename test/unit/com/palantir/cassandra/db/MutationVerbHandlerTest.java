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

package com.palantir.cassandra.db;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;

import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ArrayBackedSortedColumns;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.MutationVerbHandler;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.locator.PropertyFileSnitch;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.SchemaLoader.standardCFMD;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(OrderedJUnit4ClassRunner.class)
public class MutationVerbHandlerTest
{
    private static final String KEYSPACE = "Keyspace1";
    private static final String TABLE = "Standard1";
    private static final MutationVerbHandler HANDLER = new MutationVerbHandler();
    private static final byte KEY_OWNED_BY_A = 0xf0a;
    private static final byte KEY_OWNED_BY_B = 0xf0b;

    @BeforeClass
    public static void setUp() throws ConfigurationException
    {
        DatabaseDescriptor.setDaemonInitialized();
        IEndpointSnitch snitch = new PropertyFileSnitch();
        DatabaseDescriptor.setEndpointSnitch(snitch);
        Keyspace.setInitialized();
    }

    @Before
    public void before() throws UnknownHostException
    {
        System.setProperty("palantir_cassandra.verify_keys_on_write", "true");

        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.clearUnsafe();
        // DC1
        metadata.updateNormalToken(ByteOrderedPartitioner.instance.getTokenFactory().fromString("A"),
                                   InetAddress.getByName("127.0.0.1"));
        metadata.updateNormalToken(ByteOrderedPartitioner.instance.getTokenFactory().fromString("C"),
                                   InetAddress.getByName("127.0.0.2"));
        // DC2
        metadata.updateNormalToken(ByteOrderedPartitioner.instance.getTokenFactory().fromString("B"),
                                   InetAddress.getByName("127.0.0.4"));
        metadata.updateNormalToken(ByteOrderedPartitioner.instance.getTokenFactory().fromString("D"),
                                   InetAddress.getByName("127.0.0.5"));

        Keyspace.clear(KEYSPACE);
        KSMetaData meta = KSMetaData.newKeyspace(KEYSPACE,
                                                 NetworkTopologyStrategy.class,
                                                 ImmutableMap.of("DC1", "1", "DC2", "1"),
                                                 false,
                                                 Collections.singleton(standardCFMD(KEYSPACE, TABLE)));
        Schema.instance.setKeyspaceDefinition(meta);
    }


    @Test
    public void doVerb_appliesWriteWhenOwned() throws IOException
    {
        MessageIn<Mutation> message = getMutationMessageForKey(KEY_OWNED_BY_A);
        HANDLER.doVerb(message, 0);

        verify(message.payload).apply();
    }

    @Test(expected = RuntimeException.class)
    public void doVerb_throwsWriteWhenNotOwned() throws IOException
    {
        MessageIn<Mutation> message = getMutationMessageForKey(KEY_OWNED_BY_B);
        HANDLER.doVerb(message, 0);

        verify(message.payload, times(0)).apply();
    }

    private MessageIn<Mutation> getMutationMessageForKey(byte key) throws UnknownHostException
    {
        ColumnFamily cf = ArrayBackedSortedColumns.factory.create(KEYSPACE, TABLE);
        cf.addColumn(Util.column("c1", "v1", 0));
        Mutation mutation = spy(new Mutation(KEYSPACE, new Row(ByteBuffer.wrap(new byte[]{ key }), cf)));
        doNothing().when(mutation).apply();
        return MessageIn.create(InetAddress.getByName("127.0.0.2"),
                                mutation,
                                new HashMap<>(),
                                MessagingService.Verb.MUTATION,
                                MessagingService.current_version);
    }
}
