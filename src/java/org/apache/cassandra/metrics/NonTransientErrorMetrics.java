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

import java.util.Arrays;
import java.util.EnumMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import com.codahale.metrics.Counter;
import org.apache.cassandra.service.StorageServiceMBean;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public class NonTransientErrorMetrics
{
    public static NonTransientErrorMetrics instance = new NonTransientErrorMetrics();
    private final MetricNameFactory factory; 
    private final Map<StorageServiceMBean.NonTransientError, Counter> errorCounters;

    private NonTransientErrorMetrics()
    {
        this.factory = new DefaultNameFactory("NonTransientErrors");
        Map<StorageServiceMBean.NonTransientError, Counter> tmp = new EnumMap<>(StorageServiceMBean.NonTransientError.class);
        Arrays.stream(StorageServiceMBean.NonTransientError.values())
              .forEach(error -> tmp.put(error, Metrics.counter(factory.createMetricName(error.name().toLowerCase()))));
        this.errorCounters = ImmutableMap.copyOf(tmp);
    }

    public void record(StorageServiceMBean.NonTransientError error)
    {
        errorCounters.get(error).inc();
    }
}
