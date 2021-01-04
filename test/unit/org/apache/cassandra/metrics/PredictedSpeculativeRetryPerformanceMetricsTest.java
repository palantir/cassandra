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

package org.apache.cassandra.metrics;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.codahale.metrics.Metric;
import com.codahale.metrics.Snapshot;
import org.apache.cassandra.MockSchema;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.locator.DynamicEndpointSnitch;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.SimpleSnitch;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.service.AbstractReadExecutor;
import org.apache.cassandra.service.ReadExecutorTestUtils;
import org.mockito.Mockito;

import static org.apache.cassandra.metrics.PredictedSpeculativeRetryPerformanceMetrics.Threshold;
import static org.apache.cassandra.metrics.PredictedSpeculativeRetryPerformanceMetrics.Threshold.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PredictedSpeculativeRetryPerformanceMetricsTest {
    private static final String KEYSPACE = "ReadExecutorTestKeyspace";
    private static final String CF = "ReadExecutorTestCF";
    private static final InetAddress addr1 = mock(InetAddress.class);
    private static final InetAddress addr2 = mock(InetAddress.class);
    private static final List<InetAddress> replicas = ImmutableList.of(addr1, addr2);
    private Map<Threshold, PredictedSpeculativeRetryPerformanceMetrics> thresholdToMetrics;

    @BeforeClass
    public static void beforeClass()
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE, CF));
    }

    @Before
    public void before() {
        thresholdToMetrics = PredictedSpeculativeRetryPerformanceMetrics.createMetricsByThresholds(AbstractReadExecutor.class)
                                                                        .stream().collect(Collectors.toMap(
                                                                                            metric -> metric.threshold,
                                                                                            Mockito::spy));
        MockSchema.cleanup();
        SimpleSnitch ss = new SimpleSnitch();
        IEndpointSnitch snitch = spy(new DynamicEndpointSnitch(ss, String.valueOf(ss.hashCode())));
        Snapshot mockSnapshot = mock(Snapshot.class);
        when(mockSnapshot.get99thPercentile()).thenReturn(100.0);
        doReturn(mockSnapshot).when(snitch).getSnapshot(any());
        DatabaseDescriptor.setEndpointSnitch(snitch);
    }

    @Test
    public void testMetricsDifferentByThreshold() {
        AbstractReadExecutor executor = ReadExecutorTestUtils.getTestNeverSpeculatingReadExecutor(KEYSPACE, CF, replicas);
        PredictedSpeculativeRetryPerformanceMetrics specMetrics1 = new PredictedSpeculativeRetryPerformanceMetrics(
                            MILLISECONDS_100, executor.getClass());
        PredictedSpeculativeRetryPerformanceMetrics specMetrics2 = new PredictedSpeculativeRetryPerformanceMetrics(
                            SECONDS_5, executor.getClass());
        specMetrics1.addNano(10000L);
        specMetrics2.addNano(20000L);

        assertThat(specMetrics1.latency.getCount()).isEqualTo(1);
        assertThat(specMetrics2.latency.getCount()).isEqualTo(1);

        assertThat(specMetrics1.totalLatency.getCount()).isEqualTo(10);
        assertThat(specMetrics2.totalLatency.getCount()).isEqualTo(20);

        specMetrics1.release();
        specMetrics2.release();
    }

    @Test
    public void testMetricsDifferentByReadExecutor() {
        AbstractReadExecutor executor1 = ReadExecutorTestUtils.getTestNeverSpeculatingReadExecutor(KEYSPACE, CF, replicas);
        AbstractReadExecutor executor2 = ReadExecutorTestUtils.getTestSpeculatingReadExecutor(KEYSPACE, CF, replicas);
        PredictedSpeculativeRetryPerformanceMetrics specMetrics1 = new PredictedSpeculativeRetryPerformanceMetrics(
                                SECONDS_5, executor1.getClass());
        PredictedSpeculativeRetryPerformanceMetrics specMetrics2 = new PredictedSpeculativeRetryPerformanceMetrics(
                                SECONDS_5, executor2.getClass());
        specMetrics1.addNano(10000L);
        specMetrics2.addNano(20000L);

        assertThat(specMetrics1.latency.getCount()).isEqualTo(1);
        assertThat(specMetrics2.latency.getCount()).isEqualTo(1);

        assertThat(specMetrics1.totalLatency.getCount()).isEqualTo(10);
        assertThat(specMetrics2.totalLatency.getCount()).isEqualTo(20);
        specMetrics1.release();
        specMetrics2.release();
    }

    @Test
    public void testMetricsSameForCommonReadExecutorAndThreshold() {
        AbstractReadExecutor executor1 = ReadExecutorTestUtils.getTestNeverSpeculatingReadExecutor(KEYSPACE, CF, replicas);
        PredictedSpeculativeRetryPerformanceMetrics specMetrics1 = new PredictedSpeculativeRetryPerformanceMetrics(
                                SECONDS_5, executor1.getClass());
        PredictedSpeculativeRetryPerformanceMetrics specMetrics2 = new PredictedSpeculativeRetryPerformanceMetrics(
                                SECONDS_5, executor1.getClass());

        specMetrics1.addNano(10000L);
        specMetrics2.addNano(20000L);

        assertThat(specMetrics1.latency.getCount()).isEqualTo(2);
        assertThat(specMetrics2.latency.getCount()).isEqualTo(2);

        assertThat(specMetrics1.totalLatency.getCount()).isEqualTo(30);
        assertThat(specMetrics2.totalLatency.getCount()).isEqualTo(30);
        specMetrics1.release();
        specMetrics2.release();
    }

    @Test
    public void testMetricsRegistered() {
        AbstractReadExecutor executor = ReadExecutorTestUtils.getTestNeverSpeculatingReadExecutor(KEYSPACE, CF, replicas);
        PredictedSpeculativeRetryPerformanceMetrics specMetrics = new PredictedSpeculativeRetryPerformanceMetrics(
                            MILLISECONDS_100, executor.getClass());

        Map<String, Metric> metrics = new HashMap<>(CassandraMetricsRegistry.Metrics.getMetrics());

        String base = "org.apache.cassandra.metrics.PredictedSpeculativeRetryPerformance.%s."
                      + executor.getClass().getSimpleName() + ".MILLISECONDS_100";
        assertThat(metrics.containsKey(String.format(base, "PredictedSpeculativeRetryPerformanceLatency"))).isEqualTo(true);
        assertThat(metrics.containsKey(String.format(base, "PredictedSpeculativeRetryPerformanceTotalLatency"))).isEqualTo(true);
        specMetrics.release();
    }

    @Test
    public void testMaybeWriteMetricsIgnoresGreaterThresholds() {
        ColumnFamilyStore cfs = spy(MockSchema.newCFS());
        for (PredictedSpeculativeRetryPerformanceMetrics specMetrics : thresholdToMetrics.values()) {
            assertThat(specMetrics.maybeWriteMetrics(cfs, 10L, 0L, mock(InetAddress.class))).isFalse();
        }
    }

    @Test
    public void testMaybeWriteMetricsIncludesLowerThresholds() {
        ColumnFamilyStore cfs = spy(MockSchema.newCFS());
        cfs.metric.coordinatorReadLatency.addNano(10);
        long p99 = (long) cfs.metric.coordinatorReadLatency.getSnapshot().get99thPercentile();
        long timestamp = Math.max(TimeUnit.NANOSECONDS.convert(5, TimeUnit.SECONDS), p99) + 100;

        for (PredictedSpeculativeRetryPerformanceMetrics specMetrics : thresholdToMetrics.values()) {
            assertThat(specMetrics.maybeWriteMetrics(cfs, 0L, timestamp, mock(InetAddress.class))).isTrue();
        }
    }

    @Test
    public void testMaybeWriteMetricsHandlesMixedThresholds() {
        ColumnFamilyStore cfs = spy(MockSchema.newCFS());
        cfs.metric.coordinatorReadLatency.addNano(10);

        long p99 = (long) cfs.metric.coordinatorReadLatency.getSnapshot().get99thPercentile();
        long p95 = (long) cfs.metric.coordinatorReadLatency.getSnapshot().get95thPercentile();
        long p50 = (long) cfs.metric.coordinatorReadLatency.getSnapshot().getMedian();
        long timestamp = TimeUnit.NANOSECONDS.convert(2, TimeUnit.SECONDS);

        assertThat(thresholdToMetrics.get(MILLISECONDS_100).maybeWriteMetrics(cfs, 0L, timestamp, mock(InetAddress.class)))
                                                                                                            .isTrue();
        assertThat(thresholdToMetrics.get(SECONDS_1).maybeWriteMetrics(cfs, 0L, timestamp, mock(InetAddress.class)))
                                                                                                            .isTrue();
        assertThat(thresholdToMetrics.get(SECONDS_5).maybeWriteMetrics(cfs, 0L, timestamp, mock(InetAddress.class)))
                                                                                                            .isFalse();

        assertThat(thresholdToMetrics.get(P50).maybeWriteMetrics(cfs, 0L, timestamp, mock(InetAddress.class)))
                                                                                            .isEqualTo(p50 < timestamp);
        assertThat(thresholdToMetrics.get(P95).maybeWriteMetrics(cfs, 0L, timestamp, mock(InetAddress.class)))
                                                                                            .isEqualTo(p95 < timestamp);
        assertThat(thresholdToMetrics.get(P99).maybeWriteMetrics(cfs, 0L, timestamp, mock(InetAddress.class)))
                                                                                            .isEqualTo(p99 < timestamp);
    }

    @Test
    public void testMaybeWriteMetricsUsesCorrectLatencies() {
        ColumnFamilyStore cfs = spy(MockSchema.newCFS());
        cfs.metric.coordinatorReadLatency.addNano(10);

        long p99 = (long) cfs.metric.coordinatorReadLatency.getSnapshot().get99thPercentile();
        long p95 = (long) cfs.metric.coordinatorReadLatency.getSnapshot().get95thPercentile();
        long p50 = (long) cfs.metric.coordinatorReadLatency.getSnapshot().getMedian();
        long timestamp = TimeUnit.NANOSECONDS.convert(10 + p99, TimeUnit.SECONDS);

        for (PredictedSpeculativeRetryPerformanceMetrics specMetrics : thresholdToMetrics.values()) {
            specMetrics.maybeWriteMetrics(cfs, 10L, timestamp, mock(InetAddress.class));
        }

        verify(thresholdToMetrics.get(MILLISECONDS_100), times(1)).addNano(
                                eq(TimeUnit.NANOSECONDS.convert(100, TimeUnit.MILLISECONDS) + 100));
        verify(thresholdToMetrics.get(SECONDS_1), times(1)).addNano(
                                        eq(TimeUnit.NANOSECONDS.convert(1, TimeUnit.SECONDS) + 100));
        verify(thresholdToMetrics.get(SECONDS_5), times(1)).addNano(
                                        eq(TimeUnit.NANOSECONDS.convert(5, TimeUnit.SECONDS) + 100));

        verify(thresholdToMetrics.get(P50), times(1)).addNano(eq(p50 + 100));
        verify(thresholdToMetrics.get(P95), times(1)).addNano(eq(p95 + 100));
        verify(thresholdToMetrics.get(P99), times(1)).addNano(eq(p99 + 100));
    }

    @Test
    public void testMaybeWriteMetricsIgnoresUninitializedPercentileThresholds() {
        ColumnFamilyStore cfs = spy(MockSchema.newCFS());
        long timestamp = TimeUnit.NANOSECONDS.convert(10, TimeUnit.SECONDS);
        assertThat(thresholdToMetrics.get(P99).maybeWriteMetrics(cfs, 0L, timestamp, mock(InetAddress.class))).isFalse();
        assertThat(thresholdToMetrics.get(P95).maybeWriteMetrics(cfs, 0L, timestamp, mock(InetAddress.class))).isFalse();
        assertThat(thresholdToMetrics.get(P50).maybeWriteMetrics(cfs, 0L, timestamp, mock(InetAddress.class))).isFalse();
    }

    @Test
    public void testMaybeWriteMetricsIgnoresWhenNoSamples() {
        ColumnFamilyStore cfs = spy(MockSchema.newCFS());
        SimpleSnitch ss = new SimpleSnitch();
        IEndpointSnitch snitch = spy(new DynamicEndpointSnitch(ss, String.valueOf(ss.hashCode())));
        doThrow(new NullPointerException()).when(snitch).getSnapshot(any());
        DatabaseDescriptor.setEndpointSnitch(snitch);

        long timestamp = TimeUnit.NANOSECONDS.convert(5, TimeUnit.SECONDS) + 100;
        for (PredictedSpeculativeRetryPerformanceMetrics specMetrics : thresholdToMetrics.values()) {
            assertThat(specMetrics.maybeWriteMetrics(cfs, 0L, timestamp, mock(InetAddress.class))).isFalse();
        }
    }

    @Test
    public void testMaybeWriteMetricsIgnoresWhenUnsupportedGetSnapshot() {
        ColumnFamilyStore cfs = spy(MockSchema.newCFS());
        SimpleSnitch ss = new SimpleSnitch();
        IEndpointSnitch snitch = spy(new DynamicEndpointSnitch(ss, String.valueOf(ss.hashCode())));
        doThrow(new UnsupportedOperationException()).when(snitch).getSnapshot(any());
        DatabaseDescriptor.setEndpointSnitch(snitch);

        long timestamp = TimeUnit.NANOSECONDS.convert(5, TimeUnit.SECONDS) + 100;
        for (PredictedSpeculativeRetryPerformanceMetrics specMetrics : thresholdToMetrics.values()) {
            assertThat(specMetrics.maybeWriteMetrics(cfs, 0L, timestamp, mock(InetAddress.class))).isFalse();
        }
    }
}
