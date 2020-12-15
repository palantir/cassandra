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

import com.google.common.collect.ImmutableList;
import org.junit.BeforeClass;
import org.junit.Test;

import com.codahale.metrics.Metric;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.service.AbstractReadExecutor;
import org.apache.cassandra.service.ReadExecutorTest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class PredictedSpeculativeRetryPerformanceMetricsTest
{
    private static final String KEYSPACE = "ReadExecutorTestKeyspace";
    private static final String CF = "ReadExecutorTestCF";
    private static final InetAddress addr1 = mock(InetAddress.class);
    private static final InetAddress addr2 = mock(InetAddress.class);
    private static final List<InetAddress> replicas = ImmutableList.of(addr1, addr2);

    @BeforeClass
    public static void beforeClass()
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE, CF));
    }
    @Test
    public void testMetricsDifferentByThreshold() {
        AbstractReadExecutor executor = ReadExecutorTest.getTestNeverSpeculatingReadExecutor(KEYSPACE, CF, replicas);
        PredictedSpeculativeRetryPerformanceMetrics specMetrics1 = new PredictedSpeculativeRetryPerformanceMetrics(
                                                    AbstractReadExecutor.Threshold.MILLISECONDS_100, executor.getClass());
        PredictedSpeculativeRetryPerformanceMetrics specMetrics2 = new PredictedSpeculativeRetryPerformanceMetrics(
                                                    AbstractReadExecutor.Threshold.SECONDS_5, executor.getClass());
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
        AbstractReadExecutor executor1 = ReadExecutorTest.getTestNeverSpeculatingReadExecutor(KEYSPACE, CF, replicas);
        AbstractReadExecutor executor2 = ReadExecutorTest.getTestSpeculatingReadExecutor(KEYSPACE, CF, replicas);
        PredictedSpeculativeRetryPerformanceMetrics specMetrics1 = new PredictedSpeculativeRetryPerformanceMetrics(
                                                        AbstractReadExecutor.Threshold.SECONDS_5, executor1.getClass());
        PredictedSpeculativeRetryPerformanceMetrics specMetrics2 = new PredictedSpeculativeRetryPerformanceMetrics(
                                                        AbstractReadExecutor.Threshold.SECONDS_5, executor2.getClass());
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
        AbstractReadExecutor executor1 = ReadExecutorTest.getTestNeverSpeculatingReadExecutor(KEYSPACE, CF, replicas);
        PredictedSpeculativeRetryPerformanceMetrics specMetrics1 = new PredictedSpeculativeRetryPerformanceMetrics(
                                                        AbstractReadExecutor.Threshold.SECONDS_5, executor1.getClass());
        PredictedSpeculativeRetryPerformanceMetrics specMetrics2 = new PredictedSpeculativeRetryPerformanceMetrics(
                                                        AbstractReadExecutor.Threshold.SECONDS_5, executor1.getClass());

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
        AbstractReadExecutor executor = ReadExecutorTest.getTestNeverSpeculatingReadExecutor(KEYSPACE, CF, replicas);
        PredictedSpeculativeRetryPerformanceMetrics specMetrics = new PredictedSpeculativeRetryPerformanceMetrics(
                                                AbstractReadExecutor.Threshold.MILLISECONDS_100, executor.getClass());

        Map<String, Metric> metrics = new HashMap<>(CassandraMetricsRegistry.Metrics.getMetrics());

        String base = "org.apache.cassandra.metrics.PredictedSpeculativeRetryPerformance.%s."
                      + executor.getClass().getSimpleName() + ".MILLISECONDS_100";
        assertThat(metrics.containsKey(String.format(base, "PredictedSpeculativeRetryPerformanceLatency"))).isEqualTo(true);
        assertThat(metrics.containsKey(String.format(base, "PredictedSpeculativeRetryPerformanceTotalLatency"))).isEqualTo(true);
        specMetrics.release();
    }
}
