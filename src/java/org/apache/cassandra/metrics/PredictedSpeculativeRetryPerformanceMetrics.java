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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Snapshot;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.service.AbstractReadExecutor;

public class PredictedSpeculativeRetryPerformanceMetrics extends LatencyMetrics {
    private static final Logger logger = LoggerFactory.getLogger(PredictedSpeculativeRetryPerformanceMetrics.class);
    public static final String type = "PredictedSpeculativeRetryPerformance";
    public final Threshold threshold;

    public <T extends AbstractReadExecutor> PredictedSpeculativeRetryPerformanceMetrics(Threshold threshold, Class<T> readExecutorClass) {
        super(new PredictedSpeculativeRetryPerformanceMetricNameFactory(threshold, readExecutorClass), type);
        this.threshold = threshold;
    }

    public static <T extends AbstractReadExecutor> List<PredictedSpeculativeRetryPerformanceMetrics> createMetricsByThresholds(Class<T> readExecutorClass) {
        return Arrays.stream(PredictedSpeculativeRetryPerformanceMetrics.Threshold.values()).map(
            threshold -> new PredictedSpeculativeRetryPerformanceMetrics(threshold, readExecutorClass))
                     .collect(Collectors.toList());
    }

    public void maybeWriteMetrics(ColumnFamilyStore cfs, long start, long timestamp, InetAddress extraReplica) {
        long thresholdTime;
        TimeUnit unit;
        switch (threshold) {
            case SECONDS_5:
                thresholdTime = 5;
                unit = TimeUnit.SECONDS;
                break;
            case SECONDS_1:
                thresholdTime = 1;
                unit = TimeUnit.SECONDS;
                break;
            case MILLISECONDS_100:
                thresholdTime = 100;
                unit = TimeUnit.MILLISECONDS;
                break;
            case P50:
                thresholdTime = (long) (cfs.metric.coordinatorReadLatency.getSnapshot().getMedian());
                unit = TimeUnit.NANOSECONDS;
                break;
            case P95:
                thresholdTime = (long) (cfs.metric.coordinatorReadLatency.getSnapshot().get95thPercentile());
                unit = TimeUnit.NANOSECONDS;
                break;
            case P99:
                thresholdTime = (long) (cfs.metric.coordinatorReadLatency.getSnapshot().get99thPercentile());
                unit = TimeUnit.NANOSECONDS;
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + threshold);
        }
        if (thresholdTime < 1) {
            // Don't want uninitialized percentile latencies to skew the metrics
            return;
        }
        long extraReplicaP99Latency;
        try {
            Snapshot extraReplicaSnapshot = DatabaseDescriptor.getEndpointSnitch().getSnapshot(extraReplica);
            extraReplicaP99Latency = (long) extraReplicaSnapshot.get99thPercentile();
        } catch (UnsupportedOperationException e) {
            extraReplicaP99Latency = Long.MIN_VALUE;
        } catch (RuntimeException e) {
            logger.error("Failed to get p99 latency from endpoint snitch to record predicted speculative retry metrics", e);
            return;
        }
        thresholdTime = TimeUnit.NANOSECONDS.convert(thresholdTime, unit);
        if (timestamp - start > thresholdTime && extraReplicaP99Latency > 0) {
            this.addNano(thresholdTime + extraReplicaP99Latency);
        }
    }

    public enum Threshold
    {
        P99,
        P95,
        P50,
        SECONDS_5,
        SECONDS_1,
        MILLISECONDS_100
    }

    static class PredictedSpeculativeRetryPerformanceMetricNameFactory implements MetricNameFactory {
        private final String thresholdName;
        private final String readExecutorClassName;

        <T extends AbstractReadExecutor> PredictedSpeculativeRetryPerformanceMetricNameFactory(Threshold threshold, Class<T> readExecutorClass) {
            this.thresholdName = threshold.name();
            this.readExecutorClassName = readExecutorClass.getSimpleName();
        }

        public CassandraMetricsRegistry.MetricName createMetricName(String metricName) {
            String groupName = ColumnFamilyMetrics.class.getPackage().getName();

            StringBuilder mbeanName = new StringBuilder();
            mbeanName.append(groupName).append(":");
            mbeanName.append("type=").append(type);
            mbeanName.append(",readExecutor=").append(readExecutorClassName);
            mbeanName.append(",threshold=").append(thresholdName);
            mbeanName.append(",name=").append(metricName);

            return new CassandraMetricsRegistry.MetricName(groupName, type, metricName, readExecutorClassName + "." + thresholdName, mbeanName.toString());
        }
    }
}
