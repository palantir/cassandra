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
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import com.google.common.collect.ImmutableList;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.MockSchema;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.SliceByNamesReadCommand;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.filter.NamesQueryFilter;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.MessagingServiceTest;

import static org.assertj.core.api.Assertions.assertThat;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class ReadExecutorTest {
    private static final String KEYSPACE = "ReadExecutorTestKeyspace";
    private static final String CF = "ReadExecutorTestCF";
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
    public void testLatenciesRecordedForLocalEndpoint() {
        AbstractReadExecutor exc = spy(getTestNeverSpeculatingReadExecutor(KEYSPACE, CF, ImmutableList.of(addr1)));
        when(exc.isLocalRequest(addr1)).thenReturn(true);

        assertThat(exc.latencies).isEmpty();
        exc.executeAsync();
        assertThat(exc.latencies).hasSize(1);
    }

    @Test
    public void testLatenciesRecordedForRemoteEndpoint() throws DigestMismatchException
    {
        AbstractReadExecutor exc = spy(getTestNeverSpeculatingReadExecutor(KEYSPACE, CF, ImmutableList.of(addr2)));
        when(exc.isLocalRequest(addr2)).thenReturn(false);

        MessagingService messagingService = MessagingServiceTest.getTestService();
        doReturn(messagingService).when(exc).getMessagingServiceInstance();

        assertThat(exc.latencies).isEmpty();
        exc.executeAsync();
        exc.handler.responseForTest();
        exc.get();
        assertThat(exc.latencies).hasSize(1);
    }

    @Test
    public void testLatenciesRecordedForMultipleEndpoints() throws DigestMismatchException
    {
        AbstractReadExecutor exc = spy(getTestNeverSpeculatingReadExecutor(KEYSPACE, CF, targetReplicas));
        when(exc.isLocalRequest(addr2)).thenReturn(false);
        when(exc.isLocalRequest(addr1)).thenReturn(true);

        MessagingService messagingService = MessagingServiceTest.getTestService();
        doReturn(messagingService).when(exc).getMessagingServiceInstance();

        assertThat(exc.latencies).isEmpty();
        exc.executeAsync();
        exc.handler.responseForTest();
        exc.get();
        assertThat(exc.latencies).hasSize(2);
        for (Long latency : exc.latencies) {
            assertThat(latency).isGreaterThan(0L);
        }
    }

    public static AbstractReadExecutor getTestNeverSpeculatingReadExecutor(String keyspace, String cf, List<InetAddress> replicas) {
        ColumnFamilyStore cfs = spy(MockSchema.newCFS());
        return spy(new AbstractReadExecutor.NeverSpeculatingReadExecutor(
                                    getReadCommand(keyspace, cf), ConsistencyLevel.ANY, replicas, cfs));
    }

    public static AbstractReadExecutor getTestSpeculatingReadExecutor(String keyspace, String cf, List<InetAddress> replicas) {
        ColumnFamilyStore cfs = spy(MockSchema.newCFS());
        return spy(new AbstractReadExecutor.SpeculatingReadExecutor(cfs, getReadCommand(keyspace, cf), ConsistencyLevel.ANY, replicas));
    }

    public static ReadCommand getReadCommand(String keyspace, String cf) {
        CellNameType type = Keyspace.open(keyspace).getColumnFamilyStore(cf).getComparator();
        SortedSet<CellName> colList = new TreeSet<CellName>(type);
        colList.add(Util.cellname("col1"));
        DecoratedKey dk = Util.dk("row1");
        return new SliceByNamesReadCommand(keyspace, dk.getKey(), cf, System.currentTimeMillis(), new NamesQueryFilter(colList));
    }
}
