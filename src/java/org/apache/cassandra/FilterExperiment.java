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

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.metrics.DefaultNameFactory;
import org.apache.cassandra.metrics.MetricNameFactory;

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
            } else if (!areTrulyEqual(legacyResult, function.apply(USE_LEGACY))
                       || (legacyResult.metadata().getGcGraceSeconds() == 0
                           && areEqual(fallback.apply(USE_LEGACY), fallback.apply(USE_OPTIMIZED)))) {
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

    /**
     * Palantir: this is _super lame_, but the original code is doing something really
     * silly. What's happening is that because
     * the original code is roughly nested merge(gatherTombstones(iterators)), whereas ours is
     * nested gatherTombstones(merge(iterators)), if the next element past the last one we read
     * is a tombstone (or sequence of tombstones), they'll
     * all be gathered at that point into the return cf, regardless of whether they are ever
     * consumed from the merge iterator.
     * This is silly, because it means that if we're merging (a, b, c, d) and (rangeDelete(f, h))
     * with limit 1, then our result cf will contain (a, rangeDelete(f, h)) despite the tombstone
     * being discontinuous. And it means that if we have a lot of tombstones and we read the latest
     * entry only, we will read all of the tombstones nonetheless.
     * There are other cases of this. If I am merging (a, rangeDelete(b, g)) and (rangeDelete(c, d), rangeDelete(d, f))
     * then in the legacy code I will end up with (a, rangeDelete(b, d), rangeDelete(d, g)), whereas in the modern
     * code I will end up with (a, rangeDelete(b, g)) due to the non-commutativity of the
     * range tombstone list object.
     *
     * Empirically, this means that 0.5% of queries will do a full (and unnecessary) repair
     * for the duration of the roll. The good news is that the repairing will write such tombstones
     * into the memtable, and so this can only happen once per row because memtable DeletionInfo is
     * added directly to returnCF rather than being present in the iterator. So this only happens once.
     */
    @VisibleForTesting
    static boolean areEqual(ColumnFamily legacy, ColumnFamily modern) {
        if (areTrulyEqual(legacy, modern)) {
            return true;
        }
        DeletionInfo.InOrderTester legacyTester = legacy.inOrderDeletionTester();
        Iterator<Cell> unfiltedLegacy = legacy.iterator();
        DeletionInfo.InOrderTester modernTester = modern.inOrderDeletionTester();
        Iterator<Cell> unfilteredModern = modern.iterator();
        return Iterators.elementsEqual(Iterators.filter(unfiltedLegacy, Predicates.not(legacyTester::isDeleted)),
                                       Iterators.filter(unfilteredModern, Predicates.not(modernTester::isDeleted)));
    }

    static boolean areTrulyEqual(ColumnFamily legacy, ColumnFamily modern) {
        return ColumnFamily.digest(legacy).equals(ColumnFamily.digest(modern));
    }
}
