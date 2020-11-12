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

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.codahale.metrics.Metric;
import org.apache.cassandra.db.ConsistencyLevel;

import static org.assertj.core.api.Assertions.assertThat;

public class ConsistencyLevelRequestMetricsTest
{

    @Test
    public void consistencyLevelMetricsRequestWritesToParent() throws InterruptedException {
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

        topWrite.release();
        consistencyLevelRequestMetrics1.release();
        consistencyLevelRequestMetrics2.release();

        assertThat(topWrite.totalLatency.getCount()).isEqualTo(2*numSamples);
    }

    @Test
    public void consistencyLevelRequestMetricsAreUnique() throws InterruptedException {
        ClientRequestMetrics topWrite = new ClientRequestMetrics("Write");
        final ConsistencyLevelRequestMetrics consistencyLevelRequestMetrics1 = new ConsistencyLevelRequestMetrics(ConsistencyLevel.ALL, topWrite);
        final ConsistencyLevelRequestMetrics consistencyLevelRequestMetrics2 = new ConsistencyLevelRequestMetrics(ConsistencyLevel.QUORUM, topWrite);
        final int numSamples1 = 10000;
        final int numSamples2 = 20000;

        Runnable r1 = () -> logLatency(consistencyLevelRequestMetrics1, numSamples1);
        Runnable r2 = () -> logLatency(consistencyLevelRequestMetrics2, numSamples2);

        Thread first = new Thread(r1);
        Thread second = new Thread(r2);
        first.start();
        second.start();
        first.join();
        second.join();

        topWrite.release();
        consistencyLevelRequestMetrics1.release();
        consistencyLevelRequestMetrics2.release();

        assertThat(consistencyLevelRequestMetrics1.totalLatency.getCount()).isEqualTo(numSamples1);
        assertThat(consistencyLevelRequestMetrics2.totalLatency.getCount()).isEqualTo(numSamples2);
    }

    @Test
    public void consistencyLevelRequestRegistersMetrics() {
        String operation = "Write";
        ClientRequestMetrics topWrite = new ClientRequestMetrics(operation);
        ConsistencyLevelRequestMetrics consistencyLevelRequestMetrics = new ConsistencyLevelRequestMetrics(ConsistencyLevel.ALL, topWrite);
        Map<String, Metric> metrics = new HashMap<>(CassandraMetricsRegistry.Metrics.getMetrics());
        topWrite.release();
        consistencyLevelRequestMetrics.release();
        String base = "org.apache.cassandra.metrics.ConsistencyLevelRequest.%s.ALL.Write";
        assertThat(metrics.containsKey(String.format(base, "ConsistencyLevelRequestLatency"))).isEqualTo(true);
        assertThat(metrics.containsKey(String.format(base, "ConsistencyLevelRequestTotalLatency"))).isEqualTo(true);
        assertThat(metrics.containsKey(String.format(base, "Unavailables"))).isEqualTo(true);
        assertThat(metrics.containsKey(String.format(base, "Timeouts"))).isEqualTo(true);
        assertThat(metrics.containsKey(String.format(base, "Failures"))).isEqualTo(true);
        base = "org.apache.cassandra.metrics.ClientRequest.%s.Write";
        assertThat(metrics.containsKey(String.format(base, "Latency"))).isEqualTo(true);
        assertThat(metrics.containsKey(String.format(base, "TotalLatency"))).isEqualTo(true);
        assertThat(metrics.containsKey(String.format(base, "Unavailables"))).isEqualTo(true);
        assertThat(metrics.containsKey(String.format(base, "Timeouts"))).isEqualTo(true);
        assertThat(metrics.containsKey(String.format(base, "Failures"))).isEqualTo(true);
    }

    private void logLatency(ConsistencyLevelRequestMetrics consistencyLevelRequestMetrics, int numSamples) {
        for (int i = 0; i < numSamples; i++)
        {
            consistencyLevelRequestMetrics.addNano(1000);
        }
    }
}
