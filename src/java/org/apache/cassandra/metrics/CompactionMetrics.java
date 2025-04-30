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

import java.util.*;
import java.util.concurrent.ThreadPoolExecutor;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.palantir.cassandra.db.compaction.CompactionThroughputThrottler;

import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.CompactionTracker;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * Metrics for compaction.
 */
public class CompactionMetrics implements CompactionManager.CompactionExecutorStatsCollector
{
    public static final MetricNameFactory factory = new DefaultNameFactory("Compaction");

    /** Estimated number of compactions remaining to perform */
    public final Gauge<Integer> pendingTasks;
    /** Number of completed compactions since server [re]start */
    public final Gauge<Long> completedTasks;
    /** Total number of compactions since server [re]start */
    public final Meter totalCompactionsCompleted;
    /** Total number of bytes compacted since server [re]start */
    public final Counter bytesCompacted;
    /** Number of throttled CFs due to disk pressure */
    public final Gauge<Integer> totalThrottledTables;
    /** Total completed bytes for in-flight compactions */
    public final Gauge<Long> totalCompletedBytes;

    public CompactionMetrics(final CompactionTracker tracker, final ThreadPoolExecutor... collectors)
    {
        pendingTasks = Metrics.register(factory.createMetricName("PendingTasks"), new Gauge<Integer>()
        {
            public Integer getValue()
            {
                // add estimate number of compactions need to be done
                int n = CompactionManager.instance.getPendingCompactionTasksByKeyspaceColumnFamily().values().stream()
                                                  .mapToInt(Integer::intValue)
                                                  .sum();
                // add number of currently running compactions
                return n + tracker.getCompactions().size();
            }
        });
        completedTasks = Metrics.register(factory.createMetricName("CompletedTasks"), new Gauge<Long>()
        {
            public Long getValue()
            {
                long completedTasks = 0;
                for (ThreadPoolExecutor collector : collectors)
                    completedTasks += collector.getCompletedTaskCount();
                return completedTasks;
            }
        });
        totalCompactionsCompleted = Metrics.meter(factory.createMetricName("TotalCompactionsCompleted"));
        bytesCompacted = Metrics.counter(factory.createMetricName("BytesCompacted"));
        totalThrottledTables = Metrics.register(factory.createMetricName("TotalThrottledTables"), CompactionThroughputThrottler.instance::throttledTableCount);
        totalCompletedBytes = Metrics.register(factory.createMetricName("TotalCompletedBytes"), new Gauge<Long>()
        {
            public Long getValue()
            {
                return tracker.getTotalCompletedBytes();
            }
        });
    }

    public void finishCompaction(CompactionInfo.Holder ci)
    {
        bytesCompacted.inc(ci.getCompactionInfo().getTotal());
        totalCompactionsCompleted.mark();
    }
}
