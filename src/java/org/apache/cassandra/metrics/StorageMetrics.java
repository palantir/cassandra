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
import com.codahale.metrics.Gauge;
import org.apache.cassandra.db.Directories;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * Metrics related to Storage.
 */
public class StorageMetrics
{
    private static final MetricNameFactory factory = new DefaultNameFactory("Storage");

    public static final Counter load = Metrics.counter(factory.createMetricName("Load"));
    public static final Counter exceptions = Metrics.counter(factory.createMetricName("Exceptions"));
    public static final Counter totalHintsInProgress  = Metrics.counter(factory.createMetricName("TotalHintsInProgress"));
    public static final Counter totalHints = Metrics.counter(factory.createMetricName("TotalHints"));

    /* Tracks the most filled data_file_directory */
    public static final Gauge<Double> diskUsed = Metrics.register(factory.createMetricName("DiskUsed"), new Gauge<Double>()
    {
        public Double getValue()
        {
            return Directories.getMaxPathToUtilization().getValue();
        }
    });
}
