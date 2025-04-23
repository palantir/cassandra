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

package com.palantir.cassandra.metrics;

import com.codahale.metrics.Gauge;
import org.apache.cassandra.concurrent.SEPExecutor;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public class SEPExecutorMetrics {
    private static final String groupName = SEPExecutorMetrics.class.getPackage().getName();

    public static void register(
            String executorName, SEPExecutor executor)
    {
        Gauge<Long> tasksQueued = executor::getPendingTasks;
        Metrics.register(createMetricName(executorName, "TasksQueued"), tasksQueued);

        Gauge<Integer> activeCount = executor::getActiveCount;
        Metrics.register(createMetricName(executorName, "TasksActive"), activeCount);
    }

    private static CassandraMetricsRegistry.MetricName createMetricName(String executorName, String metricName)
    {
        return new CassandraMetricsRegistry.MetricName(groupName, executorName, metricName);
    }
}
