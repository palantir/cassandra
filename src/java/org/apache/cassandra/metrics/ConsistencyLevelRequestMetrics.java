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

import org.apache.cassandra.db.ConsistencyLevel;

public class ConsistencyLevelRequestMetrics extends ClientRequestMetrics
{
    public ConsistencyLevelRequestMetrics(ConsistencyLevel consistencyLevel, ClientRequestMetrics parent) {
        super(new ConsistencyLevelRequestMetricNameFactory(consistencyLevel, parent.getScope()), parent);
    }

    static class ConsistencyLevelRequestMetricNameFactory implements MetricNameFactory {
        private final String type = "ConsistencyLevelRequest";
        private final String consistencyLevelName;
        private final String scope;

        ConsistencyLevelRequestMetricNameFactory(ConsistencyLevel consistencyLevelName, String scope)
        {
            this.consistencyLevelName = consistencyLevelName.name();
            this.scope = scope;
        }

        public CassandraMetricsRegistry.MetricName createMetricName(String metricName) {
            String groupName = ColumnFamilyMetrics.class.getPackage().getName();

            StringBuilder mbeanName = new StringBuilder();
            mbeanName.append(groupName).append(":");
            mbeanName.append("type=").append(type);
            mbeanName.append(",consistency=").append(consistencyLevelName);
            mbeanName.append(",scope=").append(scope);
            mbeanName.append(",name=").append(metricName);

            return new CassandraMetricsRegistry.MetricName(groupName, type, metricName, scope, mbeanName.toString());
        }
    }
}
