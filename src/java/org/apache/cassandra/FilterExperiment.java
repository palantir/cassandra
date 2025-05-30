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

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import com.palantir.logsafe.SafeArg;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamily;
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
            ComparisonResult initialComparison = areEqual(legacyResult, optimizedResult);
            if (initialComparison.isEqual()) {
                successes.inc();
            } else if (!areTrulyEqual(legacyResult, function.apply(USE_LEGACY))) {
                // This case means that the data was modified between getting the legacy and optimized result. As a result, this experiment is indeterminate.
                indeterminate.inc();
            } else if ((legacyResult.metadata().getGcGraceSeconds() == 0
                           && areEqual(fallback.apply(USE_LEGACY), fallback.apply(USE_OPTIMIZED)).isEqual())) {
                indeterminate.inc();
                // TODO(lkjaerozhang): Give a better log message when I actually understand what this means.
                //  The indeterminate codepath seems to have never been hit so it's probably fine to defer for now as
                //  long as we still have signal if we do hit it.
                log.warn("Query result changed under immediate compaction but results from 60 seconds ago are identical, result is indeterminate; Legacy: {}, Optimized: {}, Legacy metadata: {}, Optimized metadata: {}",
                         legacyResult, optimizedResult, safeLoggableColumnFamilyMetadata("legacyMetadata", legacyResult), safeLoggableColumnFamilyMetadata("optimizedMetadata", optimizedResult));
            } else {
                failures.inc();
                log.warn("Comparison failure while experimenting; Legacy: {}, Optimized: {}, Comparison method: {}, Legacy metadata: {}, Optimized metadata: {}",
                         legacyResult, optimizedResult, SafeArg.of("comparisonMethod", initialComparison.name()), safeLoggableColumnFamilyMetadata("legacyMetadata", legacyResult), safeLoggableColumnFamilyMetadata("optimizedMetadata", optimizedResult));
            }
        } catch (RuntimeException e) {
            failures.inc();
            log.warn("Caught an exception while experimenting. This is probably unexpected", e);
        }
        return legacyResult;
    }

    public static boolean shouldRunExperiment() {
        return ThreadLocalRandom.current().nextDouble() <= DatabaseDescriptor.getFilterExperimentProbability();
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
    static ComparisonResult areEqual(ColumnFamily legacy, ColumnFamily modern) {
        if (areTrulyEqual(legacy, modern)) {
            return ComparisonResult.EQUAL;
        }
        if (legacy == null) {
            boolean areEqual = !iterator(modern).hasNext();
            return areEqual ? ComparisonResult.EQUAL : ComparisonResult.LEGACY_WAS_NULL;
        } else if (modern == null) {
            boolean areEqual = !iterator(legacy).hasNext();
            return areEqual ? ComparisonResult.EQUAL : ComparisonResult.MODERN_WAS_NULL;
        } else {
            return compareByIterator(iterator(legacy), iterator(modern));
        }
    }

    private static Iterator<Cell> iterator(ColumnFamily columnFamily) {
        return Iterators.filter(columnFamily.iterator(),
                                Predicates.not(columnFamily.inOrderDeletionTester()::isDeleted));
    }

    private static Optional<Cell> getNext(Iterator<Cell> iterator)
    {
        return iterator.hasNext() ? Optional.ofNullable(iterator.next()) : Optional.empty();
    }

    static boolean areTrulyEqual(ColumnFamily legacy, ColumnFamily modern) {
        return ColumnFamily.digest(legacy).equals(ColumnFamily.digest(modern));
    }

    static ComparisonResult compareByIterator(Iterator<Cell> legacyIterator, Iterator<Cell> modernIterator)
    {
        ImmutableList.Builder<String> differencesBuilder = ImmutableList.builder();
        int index = -1;
        while (legacyIterator.hasNext() || modernIterator.hasNext())
        {
            index++;
            Optional<Cell> legacy = getNext(legacyIterator);
            Optional<Cell> modern = getNext(modernIterator);

            if (legacy.isPresent() && modern.isPresent())
            {
                if (legacy.get().equals(modern.get()))
                {
                    continue;
                }
                differencesBuilder.add(notEqual(index, legacy.get(), modern.get()));
            }
            else
            {
                differencesBuilder.add(missingItem(index, legacy.isPresent(), modern.isPresent()));
            }
        }
        List<String> differences = differencesBuilder.build();

        if (differences.isEmpty()) {
            return ComparisonResult.EQUAL;
        } else {
            log.warn("Column families returned different results via iterator: {}", SafeArg.of("differences", differences));
            return ComparisonResult.NOT_EQUAL_BY_ITERATOR;
        }
    }

    private static String missingItem(int index, boolean hasLegacyCell, boolean hasModernCell) {
        return String.format("[%s: MissingItem(hasLegacyCell=%s, hasModernCell=%s)]",
                             index, hasLegacyCell, hasModernCell);
    }

    private static String notEqual(int index, Cell legacy, Cell modern) {
        return String.format("[%s: NotEqual(legacyClassName=%s, modernClassName=%s, timestampMatches=%s, nameMatches=%s, valueMatches=%s, legacySerializationFlags=%s, modernSerializationFlags=%s)]",
                             index,
                             legacy.getClass().getName(),
                             modern.getClass().getName(),
                             // These are what are compared in the .equals() implementation for AbstractCell.
                             // TODO(lkjaerozhang): Follow up with information for the other types of cells.
                             legacy.timestamp() == modern.timestamp(),
                             legacy.name() == modern.name(),
                             legacy.value() == modern.value(),
                             legacy.serializationFlags(),
                             modern.serializationFlags());
    }

    private static SafeArg safeLoggableColumnFamilyMetadata(String argName, ColumnFamily columnFamily) {
        if (columnFamily == null) {
            return SafeArg.of(argName, "null");
        }
        CFMetaData metaData = columnFamily.metadata();
        return SafeArg.of(argName, new ToStringBuilder(metaData)
        .append("cfId", metaData.cfId) // UUID
        .append("ksName", metaData.ksName) // Is a metric label
        .append("cfName", metaData.cfName) // Is a metric label
        .append("cfType", metaData.cfType) // Enum
        .append("readRepairChance", metaData.getReadRepairChance()) // visible in config
        .append("dcLocalReadRepairChance", metaData.getDcLocalReadRepairChance()) // visible in config
        .append("gcGraceSeconds", metaData.getGcGraceSeconds()) // visible in config
        .append("minCompactionThreshold", metaData.getMinCompactionThreshold()) // visible in config
        .append("maxCompactionThreshold", metaData.getMaxCompactionThreshold()) // visible in config
        .append("compactionStrategyClass", metaData.compactionStrategyClass) // Class name
        .append("bloomFilterFpChance", metaData.getBloomFilterFpChance()) // visible in config
        .append("memtableFlushPeriod", metaData.getMemtableFlushPeriod()) // visible in config
        .append("caching", metaData.getCaching()) // Enums
        .append("defaultTimeToLive", metaData.getDefaultTimeToLive()) // visible in config
        .append("minIndexInterval", metaData.getMinIndexInterval()) // visible in config
        .append("maxIndexInterval", metaData.getMaxIndexInterval()) // visible in config
        .append("speculativeRetry", metaData.getSpeculativeRetry()) // Enum and percentage
        .append("isDense", metaData.getIsDense()) // boolean
        .toString());
    }

    @VisibleForTesting
    enum ComparisonResult
    {
        EQUAL(true),
        NOT_EQUAL_BY_ITERATOR(false),
        LEGACY_WAS_NULL(false),
        MODERN_WAS_NULL(false);

        private final boolean isEqual;

        ComparisonResult(boolean isEqual)
        {
            this.isEqual = isEqual;
        }

        public boolean isEqual()
        {
            return isEqual;
        }
    }
}
