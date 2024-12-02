/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.net;

import java.net.InetAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableList;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.WriteFailureException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.service.AbstractWriteResponseHandler;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.WriteResponseHandler;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class MessagingServiceTest
{
    private static final String KEYSPACE1 = "MessagingServiceKeyspace";
    private static final String CF_STANDARD1 = "columnfamily";
    private final MessagingService messagingService = MessagingService.test();

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1));
    }

    @Test
    public void testDroppedMessages()
    {
        MessagingService.Verb verb = MessagingService.Verb.READ;

        for (int i = 0; i < 5000; i++)
            messagingService.incrementDroppedMessages(verb, i % 2 == 0);

        List<String> logs = messagingService.getDroppedMessagesLogs();
        assertEquals(1, logs.size());
        assertEquals("READ messages were dropped in last 5000 ms: 2500 for internal timeout and 2500 for cross node timeout", logs.get(0));
        assertEquals(5000, (int)messagingService.getDroppedMessages().get(verb.toString()));

        logs = messagingService.getDroppedMessagesLogs();
        assertEquals(0, logs.size());

        for (int i = 0; i < 2500; i++)
            messagingService.incrementDroppedMessages(verb, i % 2 == 0);

        logs = messagingService.getDroppedMessagesLogs();
        assertEquals("READ messages were dropped in last 5000 ms: 1250 for internal timeout and 1250 for cross node timeout", logs.get(0));
        assertEquals(7500, (int)messagingService.getDroppedMessages().get(verb.toString()));
    }

    @Test
    public void shutdown_refusesNewMessagesWhenInProgress() throws InterruptedException {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        DecoratedKey dk = Util.dk("key1");

        Mutation mutation = new Mutation(KEYSPACE1, dk.getKey());
        mutation.add(CF_STANDARD1, Util.cellname("Column1"), ByteBufferUtil.bytes("asdf"), 0);
        MessageOut<Mutation> message = mutation.createMessage();
        List<MessagingService.SocketThread> incomingAcceptThreads;
        try {
            messagingService.listen();
            assertTrue(messagingService.isListening());
            assertFalse(MessagingService.instance().isListening());
            incomingAcceptThreads = messagingService.getSocketThreads();
            assertTrue(incomingAcceptThreads.size() > 0);

            TestHandler handler = createHandler(keyspace);
            MessagingService.instance().sendRR(message, FBUtilities.getLocalAddress(), handler, false);
            handler.get();
            assertEquals(1, handler.success);
        } finally {
            messagingService.shutdown();
        }
        incomingAcceptThreads.forEach(thread -> assertFalse(thread.isAlive()));
        DatabaseDescriptor.setWriteRpcTimeout(Duration.ofSeconds(1).toMillis());
        DatabaseDescriptor.setInternodeConnectionTimeout(Duration.ofMillis(50).toMillis());

        Instant start = Instant.now();
        TestHandler handler2 = createHandler(keyspace);
        MessagingService.instance().sendRR(message, FBUtilities.getLocalAddress(), handler2, false);
        handler2.get();
        Duration handler2Time = Duration.between(start, Instant.now());
        start = Instant.now();
        TestHandler handler3 = createHandler(keyspace);
        MessagingService.instance().sendRR(message, FBUtilities.getLocalAddress(), handler3, false);
        handler3.get();
        Duration handler3Time = Duration.between(start, Instant.now());
        TestHandler handler4 = createHandler(keyspace);
        MessagingService.instance().sendRR(message, FBUtilities.getLocalAddress(), handler4, false);
        handler4.get();
        Duration handler4Time = Duration.between(start, Instant.now());

        // handler2 may or may not fail vs timeout. Likely due to OS level buffering, flushing the socket that has now
        // been closed on the receiving end may or may not throw an IOException
        int failures = handler2.failures + handler3.failures + handler4.failures;
        assertTrue(failures >= 2);

        // Failures should fail in less time than the write timeout, as they hit connect timeout instead
        assertTrue(handler3Time.minus(Duration.ofMillis(800)).isNegative());
        assertTrue(handler4Time.minus(Duration.ofMillis(800)).isNegative());
    }

    private TestHandler createHandler(Keyspace ks) {
        return new TestHandler(ImmutableList.of(FBUtilities.getLocalAddress()), ImmutableList.of(), ConsistencyLevel.ANY, ks, () -> {}, WriteType.SIMPLE);
    }

    static class TestHandler<T> extends WriteResponseHandler<T> {
        public int success = 0;
        public int failures = 0;
        public int timeouts = 0;


        public TestHandler(Collection<InetAddress> writeEndpoints, Collection<InetAddress> pendingEndpoints, ConsistencyLevel consistencyLevel, Keyspace keyspace, Runnable callback, WriteType writeType)
        {
            super(writeEndpoints, pendingEndpoints, consistencyLevel, keyspace, callback, writeType);
        }

        @Override
        protected int totalBlockFor() {
            return 1;
        }

        @Override
        public void get() {
            try {
                super.get();
                success++;
            } catch (WriteTimeoutException e) {
                timeouts++;
            } catch (WriteFailureException e) {
                failures++;
            }
        }
    }
}
