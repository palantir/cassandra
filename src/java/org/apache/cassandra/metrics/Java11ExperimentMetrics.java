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

import com.codahale.metrics.Counter;

public class Java11ExperimentMetrics
{
    private static final MetricNameFactory metricFactory = new DefaultNameFactory("J11_LOCKING");
    public static final Counter aquired = CassandraMetricsRegistry.Metrics.counter(metricFactory.createMetricName("acquired"));
    public static final Counter released = CassandraMetricsRegistry.Metrics.counter(metricFactory.createMetricName("released"));
    public static final Counter buffersCleaned = CassandraMetricsRegistry.Metrics.counter(metricFactory.createMetricName("buffer-clean"));
    public static final Counter mmapSegment = CassandraMetricsRegistry.Metrics.counter(metricFactory.createMetricName("buffer-mmap"));
    public static final Counter compressed = CassandraMetricsRegistry.Metrics.counter(metricFactory.createMetricName("buffer-compressed"));
    public static final Counter rar = CassandraMetricsRegistry.Metrics.counter(metricFactory.createMetricName("buffer-rar"));
    public static final Counter crar = CassandraMetricsRegistry.Metrics.counter(metricFactory.createMetricName("buffer-crar"));
    public static final Counter slab = CassandraMetricsRegistry.Metrics.counter(metricFactory.createMetricName("buffer-slab"));
    public static final Counter direct = CassandraMetricsRegistry.Metrics.counter(metricFactory.createMetricName("buffer-direct"));
}
