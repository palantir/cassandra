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

package com.palantir.cassandra;

import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.metrics.RestorableMeter;

public class TombstoneHotness
{
    public static final Logger log = LoggerFactory.getLogger(TombstoneHotness.class);
    // will not prefer compacting any table that we have not read at least 1MM tombstones from
    private static final long LOWER_READ_THRESHOLD = 1_000_000;
    private static final int NUMBER_OF_TABLES_ELIGIBLE_FOR_COMPACTION = 10;
    private static final Supplier<Set<SSTableReader>> hottestSstables =
        Suppliers.memoizeWithExpiration(TombstoneHotness::hottestSstablesUnmemoized, 5, TimeUnit.MINUTES)::get;

    private TombstoneHotness() {}

    private static Set<SSTableReader> hottestSstablesUnmemoized() {
        PriorityQueue<Map.Entry<SSTableReader, Double>> sstables = new PriorityQueue<>(ReaderComparator.INSTANCE);
        for (Keyspace keyspace : Keyspace.nonSystem()) {
            for (ColumnFamilyStore store : keyspace.getColumnFamilyStores()) {
                for (SSTableReader sstable : store.getSSTables()) {
                    if (sstable.getTombstoneReadMeter() != null
                            && sstable.getTombstoneReadMeter().count() >= LOWER_READ_THRESHOLD) {
                        sstables.add(Maps.immutableEntry(sstable, sstable.getTombstoneReadMeter().meanRate()));
                    }
                }
            }
        }
        ImmutableSet.Builder<SSTableReader> result = ImmutableSet.builder();
        for (int i = 0; i < NUMBER_OF_TABLES_ELIGIBLE_FOR_COMPACTION && !sstables.isEmpty(); i++) {
            result.add(sstables.remove().getKey());
        }
        return result.build();
    }

    public static Set<SSTableReader> maybeGetHotSstables(Iterable<SSTableReader> readers) {
        Set<SSTableReader> result = new HashSet<>();
        Set<SSTableReader> hottest = hottestSstables.get();
        for (SSTableReader reader : readers) {
            if (hottest.contains(reader)) {
                result.add(reader);
            }
        }
        return result;
    }

    private enum ReaderComparator implements Comparator<Map.Entry<SSTableReader, Double>> {
        INSTANCE;

        @Override
        public int compare(Map.Entry<SSTableReader, Double> left, Map.Entry<SSTableReader, Double> right)
        {
            // reverse - more reads comes first
            return -Double.compare(left.getValue(), right.getValue());
        }
    }
}
