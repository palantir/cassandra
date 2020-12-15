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

import org.apache.cassandra.service.AbstractReadExecutor;

public class PredictedSpeculativeRetryPerformanceMetrics extends LatencyMetrics {
    public static final String type = "PredictedSpeculativeRetryPerformance";

    public <T extends AbstractReadExecutor> PredictedSpeculativeRetryPerformanceMetrics(AbstractReadExecutor.Threshold threshold, Class<T> readExecutorClass) {
        super(new PredictedSpeculativeRetryPerformanceMetricNameFactory(threshold, readExecutorClass), type);
    }

    static class PredictedSpeculativeRetryPerformanceMetricNameFactory implements MetricNameFactory {
        private final String thresholdName;
        private final String readExecutorClassName;

        <T extends AbstractReadExecutor> PredictedSpeculativeRetryPerformanceMetricNameFactory(AbstractReadExecutor.Threshold threshold, Class<T> readExecutorClass) {
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
