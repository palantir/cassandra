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

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.google.common.collect.ImmutableList;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.locator.SimpleStrategy;

import static org.mockito.Mockito.mock;

public class ReadCallbackTest {

    private static final String KEYSPACE = "ReadCallbackTestKeyspace";
    private static final String CF = "ReadCallbackTestCF";
    private static final InetAddress addr1 = mock(InetAddress.class);
    private static final InetAddress addr2 = mock(InetAddress.class);
    private static final List<InetAddress> targetReplicas = ImmutableList.of(addr1, addr2);
    private static final RowDigestResolver resolver = mock(RowDigestResolver.class);

    @BeforeClass
    public static void beforeClass() {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE, CF));
    }

    @Test
    public void testLatenciesAdded() throws DigestMismatchException {
        List<Long> latencies = new ArrayList<>();
        assertThat(latencies).isEmpty();
        ReadCallback<ReadResponse, Row> handler = spy(new ReadCallback<>(resolver,
                                                                         ConsistencyLevel.ANY,
                                                                         ReadExecutorTest.getReadCommand(KEYSPACE, CF),
                                                                         targetReplicas,
                                                                         Optional.of(latencies)));
        doReturn(true).when(handler).await(anyLong(), any());
        handler.get();
        assertThat(latencies).hasSize(1);
    }

    @Test
    public void testLatenciesIgnoredWhenAbsent() throws DigestMismatchException {
        ReadCallback<ReadResponse, Row> handler = spy(new ReadCallback<>(resolver,
                                                                         ConsistencyLevel.ANY,
                                                                         ReadExecutorTest.getReadCommand(KEYSPACE, CF),
                                                                         targetReplicas,
                                                                         Optional.empty()));
        doReturn(true).when(handler).await(anyLong(), any());
        handler.get();
    }

    @Test
    public void testParallelLatenciesAdded() {
        List<Long> latencies = new ArrayList<>();
        assertThat(latencies).isEmpty();
        ReadCallback<ReadResponse, Row> handler1 = spy(new ReadCallback<>(resolver,
                                                                          ConsistencyLevel.ANY,
                                                                          ReadExecutorTest.getReadCommand(KEYSPACE, CF),
                                                                          targetReplicas,
                                                                          Optional.of(latencies)));
        ReadCallback<ReadResponse, Row> handler2 = spy(new ReadCallback<>(resolver,
                                                                          ConsistencyLevel.ANY,
                                                                          ReadExecutorTest.getReadCommand(KEYSPACE, CF),
                                                                          targetReplicas,
                                                                          Optional.of(latencies)));
        doReturn(true).when(handler1).await(anyLong(), any());
        doReturn(true).when(handler2).await(anyLong(), any());
        Runnable get1 = getRunnable(handler1);
        Runnable get2 = getRunnable(handler2);

        get1.run();
        get2.run();

        assertThat(latencies).hasSize(2);
    }

    public void testLatenciesNonNegative() {
        List<Long> latencies = new ArrayList<>();
        assertThat(latencies).isEmpty();
        ReadCallback<ReadResponse, Row> handler1 = spy(new ReadCallback<>(resolver,
                                                                          ConsistencyLevel.ANY,
                                                                          ReadExecutorTest.getReadCommand(KEYSPACE, CF),
                                                                          targetReplicas,
                                                                          Optional.of(latencies)));
        ReadCallback<ReadResponse, Row> handler2 = spy(new ReadCallback<>(resolver,
                                                                          ConsistencyLevel.ANY,
                                                                          ReadExecutorTest.getReadCommand(KEYSPACE, CF),
                                                                          targetReplicas,
                                                                          Optional.of(latencies)));
        doReturn(true).when(handler1).await(anyLong(), any());
        doReturn(true).when(handler2).await(anyLong(), any());
        Runnable get1 = getRunnable(handler1);
        Runnable get2 = getRunnable(handler2);

        get1.run();
        get2.run();

        for (Long latency : latencies) {
            assertThat(latency).isGreaterThan(0L);
        }
    }

    private static Runnable getRunnable(ReadCallback<ReadResponse, Row> handler) {
        return () -> {
            try {
                handler.get();
            }
            catch (DigestMismatchException ignored) {}
        };
    }
}
