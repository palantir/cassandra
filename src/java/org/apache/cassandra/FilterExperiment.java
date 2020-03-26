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

package org.apache.cassandra;

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.metrics.DefaultNameFactory;
import org.apache.cassandra.metrics.MetricNameFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public enum FilterExperiment
{
    USE_LEGACY, USE_OPTIMIZED;

    private static final Logger log = LoggerFactory.getLogger(FilterExperiment.class);
    private static final MetricNameFactory names = new DefaultNameFactory("FilterExperiment");
    private static final Timer legacyTimer =
            CassandraMetricsRegistry.Metrics.timer(names.createMetricName("Legacy"));
    private static final Timer optimizedTimer =
            CassandraMetricsRegistry.Metrics.timer(names.createMetricName("Optimized"));
    private static final Counter successes =
            CassandraMetricsRegistry.Metrics.counter(names.createMetricName("Successes"));
    private static final Counter failures =
            CassandraMetricsRegistry.Metrics.counter(names.createMetricName("Failures"));
    private static final Counter indeterminate =
            CassandraMetricsRegistry.Metrics.counter(names.createMetricName("Indeterminate"));

    public static ColumnFamily execute(
            Function<FilterExperiment, ColumnFamily> function,
            Function<FilterExperiment, ColumnFamily> fallback) {
        if (!shouldRunExperiment()) {
            return function.apply(USE_LEGACY);
        }
        ColumnFamily legacyResult = time(() -> function.apply(USE_LEGACY), legacyTimer);
        try {
            ColumnFamily optimizedResult = time(() -> function.apply(USE_OPTIMIZED), optimizedTimer);
            if (areEqual(legacyResult, optimizedResult)) {
                successes.inc();
            } else if (areEqual(legacyResult, function.apply(USE_LEGACY))
                       || areEqual(fallback.apply(USE_LEGACY), fallback.apply(USE_OPTIMIZED))) {
                indeterminate.inc();
            } else {
                failures.inc();
                log.warn("Comparison failure while experimenting; Legacy: {}, Optimized: {}",
                         legacyResult, optimizedResult);
            }
        } catch (RuntimeException e) {
            failures.inc();
            log.warn("Caught an exception while experimenting. This is probably unexpected", e);
        }
        return legacyResult;
    }

    public static boolean shouldRunExperiment() {
        return ThreadLocalRandom.current().nextDouble() <= 0.01;
    }

    private static <T> T time(Supplier<T> delegate, Timer timer) {
        try (Timer.Context context = timer.time()) {
            return delegate.get();
        }
    }

    private static boolean areEqual(ColumnFamily legacy, ColumnFamily modern) {
        return ColumnFamily.digest(legacy).equals(ColumnFamily.digest(modern));
    }
}
