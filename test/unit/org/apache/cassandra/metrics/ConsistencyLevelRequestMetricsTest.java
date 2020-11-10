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

import java.util.Map;

import org.junit.Test;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import org.apache.cassandra.db.ConsistencyLevel;

import static org.assertj.core.api.Assertions.assertThat;

public class ConsistencyLevelRequestMetricsTest
{
    @Test
    public void consistencyLevelMetricsRequestWritesToParent() throws InterruptedException
    {
        ClientRequestMetrics topWrite = new ClientRequestMetrics("Write");
        final ConsistencyLevelRequestMetrics consistencyLevelRequestMetrics1 = new ConsistencyLevelRequestMetrics(ConsistencyLevel.ALL, topWrite);
        final ConsistencyLevelRequestMetrics consistencyLevelRequestMetrics2 = new ConsistencyLevelRequestMetrics(ConsistencyLevel.QUORUM, topWrite);
        final int numSamples = 10000;
        Runnable r1 = () -> logLatency(consistencyLevelRequestMetrics1, numSamples);
        Runnable r2 = () -> logLatency(consistencyLevelRequestMetrics2, numSamples);

        Thread first = new Thread(r1);
        Thread second = new Thread(r2);
        first.start();
        second.start();
        first.join();
        second.join();

        assertThat(topWrite.totalLatency.getCount()).isEqualTo(2*numSamples);
    }

    @Test
    public void consistencyLevelRequestRegistersMetrics() {
        ClientRequestMetrics topWrite = new ClientRequestMetrics("Write");
        new ConsistencyLevelRequestMetrics(ConsistencyLevel.ALL, topWrite);
        Map<String, Metric> existing = CassandraMetricsRegistry.Metrics.getMetrics();
        String group = "org.apache.cassandra.metrics.ConsistencyLevelRequest.%s.write";
        existing.get(String.format(group, "ClientRequestLatency"));
        existing.get(String.format(group, "ClientRequestTotalLatency"));
        existing.get(String.format(group, "Unavailables"));
        existing.get(String.format(group, "Timeouts"));
        existing.get(String.format(group, "Failures"));
    }

    private void logLatency(ConsistencyLevelRequestMetrics consistencyLevelRequestMetrics, int numSamples) {
        for (int i = 0; i < numSamples; i++)
        {
            consistencyLevelRequestMetrics.addNano(1000);
        }
    }
}
