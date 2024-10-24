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

import java.net.InetAddress;

import com.google.common.net.InetAddresses;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import org.apache.cassandra.gms.FailureDetector.ArrivalWindow;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public class FailureDetectorMetrics
{
    public static void register(InetAddress ep, ArrivalWindow window)
    {
        Metrics.register(createMetricName(ep, "phi"), (Gauge<Double>) window::getLastReportedPhi);
        Metrics.register(createMetricName(ep, "interval_histogram"), new Histogram(window));
        Metrics.register(createMetricName(ep, "last_interval"), (Gauge<Long>) window::getLastInterval);
    }

    public static void unregister(InetAddress ep)
    {
        Metrics.remove(createMetricName(ep, "phi"));
        Metrics.remove(createMetricName(ep, "interval_histogram"));
        Metrics.remove(createMetricName(ep, "last_interval"));
    }

    private static CassandraMetricsRegistry.MetricName createMetricName(InetAddress ep, String name)
    {
        String endpoint = InetAddresses.toAddrString(ep);
        String groupName = FailureDetectorMetrics.class.getPackage().getName();

        StringBuilder mbeanName = new StringBuilder();
        mbeanName.append(groupName).append(":");
        mbeanName.append("type=FailureDetector");
        mbeanName.append(",endpoint=").append(endpoint);
        mbeanName.append(",name=").append(name);

        return new CassandraMetricsRegistry.MetricName(groupName, "FailureDetector", name, endpoint, mbeanName.toString());
    }
}
