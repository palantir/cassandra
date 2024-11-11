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
import java.util.Optional;
import java.util.function.Supplier;

import com.google.common.net.InetAddresses;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Snapshot;
import com.sun.istack.internal.NotNull;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public class FailureDetectorMetrics
{
    private static final String groupName = FailureDetectorMetrics.class.getPackage().getName();
    static
    {
        Metrics.register(
            createMetricName("FailureDetectorPhiThreshold"), new Gauge<Double>()
        {
            public Double getValue()
            {
                return DatabaseDescriptor.getPhiConvictThreshold();
            }
        });
    }

    public static void register(
        InetAddress ep, Gauge<Double> phiSupplier, Gauge<Long> lastIntervalSupplier, Supplier<Snapshot> snapshotSupplier)
    {
        Metrics.register(createMetricName(ep, "FailureDetectorPhi"), phiSupplier);
        Metrics.register(createMetricName(ep, "FailureDetectorLastInterval"), lastIntervalSupplier);
        Metrics.register(createMetricName(ep, "FailureDetectorArrivalIntervals"), new ReadOnlyHistogram(snapshotSupplier));
    }

    public static void unregister(InetAddress ep)
    {
        Metrics.remove(createMetricName(ep, "FailureDetectorPhi"));
        Metrics.remove(createMetricName(ep, "FailureDetectorLastInterval"));
        Metrics.remove(createMetricName(ep, "FailureDetectorArrivalIntervals"));
    }

    private static CassandraMetricsRegistry.MetricName createMetricName(String name)
    {
        return new CassandraMetricsRegistry.MetricName(groupName, "FailureDetector", name, mBeanName("", name));
    }

    private static CassandraMetricsRegistry.MetricName createMetricName(@NotNull InetAddress ep, String name)
    {
        String endpoint = InetAddresses.toAddrString(ep);
        return new CassandraMetricsRegistry.MetricName(groupName, "FailureDetector", name, endpoint, mBeanName(endpoint, name));
    }

    private static String mBeanName(String endpoint, String name)
    {
        StringBuilder mbeanName = new StringBuilder();
        mbeanName.append(groupName).append(":");
        mbeanName.append("type=FailureDetector");
        if(!endpoint.isEmpty())
            mbeanName.append(",endpoint=").append(endpoint);
        mbeanName.append(",name=").append(name);
        return mbeanName.toString();
    }
}
