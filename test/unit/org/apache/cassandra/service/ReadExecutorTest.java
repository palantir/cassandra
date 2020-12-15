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
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.MockSchema;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
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
import org.apache.cassandra.locator.DynamicEndpointSnitch;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.SimpleSnitch;
import org.apache.cassandra.locator.SimpleStrategy;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class ReadExecutorTest {
    private static final String KEYSPACE = "ReadExecutorTestKeyspace";
    private static final String CF = "ReadExecutorTestCF";
    private static final InetAddress addr1 = mock(InetAddress.class);
    private static final InetAddress addr2 = mock(InetAddress.class);
    private static final List<InetAddress> replicas = ImmutableList.of(addr1, addr2);

    @BeforeClass
    public static void beforeClass() {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE, CF));
    }

    @Before
    public void before() {
        MockSchema.cleanup();
        SimpleSnitch ss = new SimpleSnitch();
        IEndpointSnitch snitch = spy(new DynamicEndpointSnitch(ss, String.valueOf(ss.hashCode())));
        doReturn(100L).when(snitch).getP99Latency(any());
        DatabaseDescriptor.setEndpointSnitch(snitch);
    }

    @Test
    public void testWritePredictedSpeculativeRetryPerformanceMetricsDoesNotInvokeGreaterThresholds() {
        ColumnFamilyStore cfs = spy(MockSchema.newCFS());

        AbstractReadExecutor executor = spy(new AbstractReadExecutor.NeverSpeculatingReadExecutor(
                                    getReadCommand(KEYSPACE, CF), ConsistencyLevel.ANY, replicas, cfs, 10L));
        executor.writePredictedSpeculativeRetryPerformanceMetrics(TimeUnit.NANOSECONDS.convert(0, TimeUnit.SECONDS));
        verify(executor, never()).invokePredSpecRetryPerformanceMetricWriter(any(), anyLong());
    }

    @Test
    public void testWritePredictedSpeculativeRetryPerformanceMetricsUsesCorrectLatency() {
        ColumnFamilyStore cfs = spy(MockSchema.newCFS());
        AbstractReadExecutor executor = spy(new AbstractReadExecutor.NeverSpeculatingReadExecutor(
        getReadCommand(KEYSPACE, CF), ConsistencyLevel.ANY, replicas, cfs, 0L));
        cfs.metric.coordinatorReadLatency.addNano(10);

        long p99 = (long) cfs.metric.coordinatorReadLatency.getSnapshot().get99thPercentile();
        long p95 = (long) cfs.metric.coordinatorReadLatency.getSnapshot().get95thPercentile();
        long p50 = (long) cfs.metric.coordinatorReadLatency.getSnapshot().getMedian();
        long timestamp = TimeUnit.NANOSECONDS.convert(10 + p99, TimeUnit.SECONDS);
        executor.writePredictedSpeculativeRetryPerformanceMetrics(timestamp);

        verify(executor, times(1)).invokePredSpecRetryPerformanceMetricWriter(eq(AbstractReadExecutor.Threshold.MILLISECONDS_100),
                                                                              eq(TimeUnit.NANOSECONDS.convert(100, TimeUnit.MILLISECONDS) + 100));
        verify(executor, times(1)).invokePredSpecRetryPerformanceMetricWriter(eq(AbstractReadExecutor.Threshold.SECONDS_1),
                                                                              eq(TimeUnit.NANOSECONDS.convert(1, TimeUnit.SECONDS) + 100));
        verify(executor, times(1)).invokePredSpecRetryPerformanceMetricWriter(eq(AbstractReadExecutor.Threshold.SECONDS_5),
                                                                              eq(TimeUnit.NANOSECONDS.convert(5, TimeUnit.SECONDS) + 100));

        verify(executor, times(1)).invokePredSpecRetryPerformanceMetricWriter(eq(AbstractReadExecutor.Threshold.P50), eq(p50 + 100));
        verify(executor, times(1)).invokePredSpecRetryPerformanceMetricWriter(eq(AbstractReadExecutor.Threshold.P95), eq(p95 + 100));
        verify(executor, times(1)).invokePredSpecRetryPerformanceMetricWriter(eq(AbstractReadExecutor.Threshold.P99), eq(p99 + 100));
    }

    @Test
    public void testWritePredictedSpeculativeRetryPerformanceMetricsInvokesLowerThresholds() {
        ColumnFamilyStore cfs = spy(MockSchema.newCFS());
        AbstractReadExecutor executor = spy(new AbstractReadExecutor.NeverSpeculatingReadExecutor(
                                    getReadCommand(KEYSPACE, CF), ConsistencyLevel.ANY, replicas, cfs, 10L));
        cfs.metric.coordinatorReadLatency.addNano(10);

        long p99 = (long) cfs.metric.coordinatorReadLatency.getSnapshot().get99thPercentile();
        long timestamp = Math.max(TimeUnit.NANOSECONDS.convert(5, TimeUnit.SECONDS), p99) + 100;
        executor.writePredictedSpeculativeRetryPerformanceMetrics(timestamp);

        verify(executor, times(1)).invokePredSpecRetryPerformanceMetricWriter(eq(AbstractReadExecutor.Threshold.MILLISECONDS_100), anyLong());
        verify(executor, times(1)).invokePredSpecRetryPerformanceMetricWriter(eq(AbstractReadExecutor.Threshold.SECONDS_1), anyLong());
        verify(executor, times(1)).invokePredSpecRetryPerformanceMetricWriter(eq(AbstractReadExecutor.Threshold.SECONDS_5), anyLong());
        verify(executor, times(1)).invokePredSpecRetryPerformanceMetricWriter(eq(AbstractReadExecutor.Threshold.P50), anyLong());
        verify(executor, times(1)).invokePredSpecRetryPerformanceMetricWriter(eq(AbstractReadExecutor.Threshold.P95), anyLong());
        verify(executor, times(1)).invokePredSpecRetryPerformanceMetricWriter(eq(AbstractReadExecutor.Threshold.P99), anyLong());
    }

    @Test
    public void testWritePredictedSpeculativeRetryPerformanceMetricsHandlesMixedThresholds() {
        ColumnFamilyStore cfs = spy(MockSchema.newCFS());
        AbstractReadExecutor executor = spy(new AbstractReadExecutor.NeverSpeculatingReadExecutor(
                                    getReadCommand(KEYSPACE, CF), ConsistencyLevel.ANY, replicas, cfs, 10L));
        cfs.metric.coordinatorReadLatency.addNano(10);

        long p99 = (long) cfs.metric.coordinatorReadLatency.getSnapshot().get99thPercentile();
        long p95 = (long) cfs.metric.coordinatorReadLatency.getSnapshot().get95thPercentile();
        long p50 = (long) cfs.metric.coordinatorReadLatency.getSnapshot().getMedian();
        long timestamp = TimeUnit.NANOSECONDS.convert(2, TimeUnit.SECONDS);
        executor.writePredictedSpeculativeRetryPerformanceMetrics(timestamp);

        verify(executor, times(1)).invokePredSpecRetryPerformanceMetricWriter(eq(AbstractReadExecutor.Threshold.MILLISECONDS_100), anyLong());
        verify(executor, times(1)).invokePredSpecRetryPerformanceMetricWriter(eq(AbstractReadExecutor.Threshold.SECONDS_1), anyLong());
        verify(executor, never()).invokePredSpecRetryPerformanceMetricWriter(eq(AbstractReadExecutor.Threshold.SECONDS_5), anyLong());

        verify(executor, times((p50 < timestamp) ? 1 : 0)).invokePredSpecRetryPerformanceMetricWriter(eq(AbstractReadExecutor.Threshold.P50), anyLong());
        verify(executor, times((p95 < timestamp) ? 1 : 0)).invokePredSpecRetryPerformanceMetricWriter(eq(AbstractReadExecutor.Threshold.P95), anyLong());
        verify(executor, times((p99 < timestamp) ? 1 : 0)).invokePredSpecRetryPerformanceMetricWriter(eq(AbstractReadExecutor.Threshold.P99), anyLong());
    }

    @Test
    public void testWritePredictedSpeculativeRetryPerformanceMetricsDoesNotWriteWithNoSamples() {
        ColumnFamilyStore cfs = spy(MockSchema.newCFS());
        SimpleSnitch ss = new SimpleSnitch();
        IEndpointSnitch snitch = spy(new DynamicEndpointSnitch(ss, String.valueOf(ss.hashCode())));
        doThrow(new NullPointerException()).when(snitch).getP99Latency(any());
        DatabaseDescriptor.setEndpointSnitch(snitch);

        AbstractReadExecutor executor = spy(new AbstractReadExecutor.NeverSpeculatingReadExecutor(
        getReadCommand(KEYSPACE, CF), ConsistencyLevel.ANY, replicas, cfs, 10L));
        executor.writePredictedSpeculativeRetryPerformanceMetrics(TimeUnit.NANOSECONDS.convert(0, TimeUnit.SECONDS));
        verify(executor, never()).invokePredSpecRetryPerformanceMetricWriter(any(), anyLong());
    }

    @Test
    public void testWritePredictedSpeculativeRetryPerformanceMetricsDoesNotWriteWithNegativeSampleP99() {
        ColumnFamilyStore cfs = spy(MockSchema.newCFS());
        cfs.metric.coordinatorReadLatency.addNano(10);
        SimpleSnitch ss = new SimpleSnitch();
        IEndpointSnitch snitch = spy(new DynamicEndpointSnitch(ss, String.valueOf(ss.hashCode())));
        doReturn(Long.MIN_VALUE).when(snitch).getP99Latency(any());
        DatabaseDescriptor.setEndpointSnitch(snitch);

        AbstractReadExecutor executor = spy(new AbstractReadExecutor.NeverSpeculatingReadExecutor(
        getReadCommand(KEYSPACE, CF), ConsistencyLevel.ANY, replicas, cfs, 10L));
        executor.writePredictedSpeculativeRetryPerformanceMetrics(TimeUnit.NANOSECONDS.convert(0, TimeUnit.SECONDS));
        verify(executor, never()).invokePredSpecRetryPerformanceMetricWriter(any(), anyLong());
    }

    @Test
    public void testWritePredictedSpeculativeRetryPerformanceMetricsDoesNotWriteUninitializedPercentileThresholds() {
        ColumnFamilyStore cfs = spy(MockSchema.newCFS());

        AbstractReadExecutor executor = spy(new AbstractReadExecutor.NeverSpeculatingReadExecutor(
                                        getReadCommand(KEYSPACE, CF), ConsistencyLevel.ANY, replicas, cfs, 10L));
        executor.writePredictedSpeculativeRetryPerformanceMetrics(TimeUnit.NANOSECONDS.convert(10, TimeUnit.SECONDS));
        verify(executor, never()).invokePredSpecRetryPerformanceMetricWriter(eq(AbstractReadExecutor.Threshold.P99), anyLong());
        verify(executor, never()).invokePredSpecRetryPerformanceMetricWriter(eq(AbstractReadExecutor.Threshold.P95), anyLong());
        verify(executor, never()).invokePredSpecRetryPerformanceMetricWriter(eq(AbstractReadExecutor.Threshold.P50), anyLong());
        verify(executor, times(1)).invokePredSpecRetryPerformanceMetricWriter(eq(AbstractReadExecutor.Threshold.SECONDS_5), anyLong());
    }

    public static AbstractReadExecutor getTestNeverSpeculatingReadExecutor(String keyspace, String cf, List<InetAddress> replicas) {
        ColumnFamilyStore cfs = spy(MockSchema.newCFS());
        return spy(new AbstractReadExecutor.NeverSpeculatingReadExecutor(
                                    getReadCommand(keyspace, cf), ConsistencyLevel.ANY, replicas, cfs, 0L));
    }

    public static AbstractReadExecutor getTestSpeculatingReadExecutor(String keyspace, String cf, List<InetAddress> replicas) {
        ColumnFamilyStore cfs = spy(MockSchema.newCFS());
        return spy(new AbstractReadExecutor.SpeculatingReadExecutor(cfs, getReadCommand(keyspace, cf), ConsistencyLevel.ANY, replicas));
    }

    private static ReadCommand getReadCommand(String keyspace, String cf) {
        CellNameType type = Keyspace.open(keyspace).getColumnFamilyStore(cf).getComparator();
        SortedSet<CellName> colList = new TreeSet<CellName>(type);
        colList.add(Util.cellname("col1"));
        DecoratedKey dk = Util.dk("row1");
        return new SliceByNamesReadCommand(keyspace, dk.getKey(), cf, System.currentTimeMillis(), new NamesQueryFilter(colList));
    }
}
