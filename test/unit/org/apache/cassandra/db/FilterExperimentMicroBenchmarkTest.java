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

package org.apache.cassandra.db;

import org.apache.cassandra.FilterExperiment;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.cache.CachingOptions;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.composites.Composites;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.cassandra.Util.*;

/**
 * This class tests resumable range scans at the local level, that is, within a node
 * The two entry points of interest are ColumnFamilyStore#getColumnFamily(QueryFilter filter) and ColumnFamilyStore#getRangeSlice(ExtendedFilter filter),
 * which are used by SliceFromReadCommand and RangeSliceCommand, respectively.
 */
public class FilterExperimentMicroBenchmarkTest
{
    public static final int EXPERIMENTS = 50;
    public static final int COLUMN_COUNT = 1000;

    public static final String KEYSPACE = "KeyspaceForTest";
    public static final String COLUMN_FAMILY_1 = "ColumnFamilyForTest1";
    public static final String COLUMN_FAMILY_2 = "ColumnFamilyForTest2";
    public static final DecoratedKey ROW_KEY = Util.dk("row_key");

    public static ColumnFamilyStore cfs1;
    public static ColumnFamilyStore cfs2;

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                SimpleStrategy.class,
                KSMetaData.optsWithRF(1),
                SchemaLoader.standardCFMD(KEYSPACE, COLUMN_FAMILY_1).caching(CachingOptions.NONE),
                SchemaLoader.standardCFMD(KEYSPACE, COLUMN_FAMILY_2).caching(CachingOptions.NONE)
        );

        cfs1 = Keyspace.open(KEYSPACE).getColumnFamilyStore(COLUMN_FAMILY_1);
        cfs2 = Keyspace.open(KEYSPACE).getColumnFamilyStore(COLUMN_FAMILY_2);
        cfs1.clearUnsafe();
        cfs2.clearUnsafe();
        cfs1.disableAutoCompaction();
        cfs2.disableAutoCompaction();

        List<String> cols = IntStream.rangeClosed(1, COLUMN_COUNT)
                .boxed()
                .map(String::valueOf).sorted().collect(Collectors.toList());

        for (int i = COLUMN_COUNT - 1; i >= 0; i--)
        {
            putColStandard(
                    cfs1,
                    ROW_KEY,
                    column(cols.get(i), "value", System.currentTimeMillis())
            );
            deleteRange(
                    cfs1,
                    ROW_KEY,
                    tombstone(cols.get(i), cols.get(cols.size() - 1), System.currentTimeMillis(), (int) System.currentTimeMillis())
            );

            putColStandard(
                    cfs2,
                    ROW_KEY,
                    column(cols.get(i), "value", System.currentTimeMillis())
            );
            deleteRange(
                    cfs2,
                    ROW_KEY,
                    tombstone(cols.get(i), cols.get(cols.size() - 1), System.currentTimeMillis(), (int) System.currentTimeMillis())
            );
        }
        putColStandard(
                cfs1,
                ROW_KEY,
                column(String.valueOf(0), "value", System.currentTimeMillis())
        );
        putColStandard(
                cfs2,
                ROW_KEY,
                column(String.valueOf(0), "value", System.currentTimeMillis())
        );
    }

    @Test
    public void testMicroBenchmarkLegacyFirst()
    {
        QueryFilter queryFilterAll1 = createQueryFilter(
                Composites.EMPTY,
                Composites.EMPTY,
                COLUMN_FAMILY_1,
                0
        );
        QueryFilter queryFilterAll2 = createQueryFilter(
                Composites.EMPTY,
                Composites.EMPTY,
                COLUMN_FAMILY_2,
                0
        );
        for (int i = 0; i < EXPERIMENTS; i++)
        {
            long legacy = time(() -> cfs1.getColumnFamily(queryFilterAll1, Optional.of(FilterExperiment.USE_LEGACY)));
            long optimized = time(() -> cfs2.getColumnFamily(queryFilterAll2, Optional.of(FilterExperiment.USE_OPTIMIZED)));
            System.out.println("Legacy Run " + (i + 1) + ": " + legacy / 1_000_000 + " ms");
            System.out.println("Optimized Run " + (i + 1) + ": " + optimized / 1_000_000 + " ms");
            System.out.println("Legacy - Optimized " + (i + 1) + ": " + (legacy - optimized) / 1_000_000 + " ms");
        }
    }

    @Test
    public void testMicroBenchmarkOptimizedFirst()
    {
        QueryFilter queryFilterAll1 = createQueryFilter(
                Composites.EMPTY,
                Composites.EMPTY,
                COLUMN_FAMILY_1,
                0
        );
        QueryFilter queryFilterAll2 = createQueryFilter(
                Composites.EMPTY,
                Composites.EMPTY,
                COLUMN_FAMILY_2,
                0
        );
        for (int i = 0; i < EXPERIMENTS; i++)
        {
            long optimized = time(() -> cfs2.getColumnFamily(queryFilterAll2, Optional.of(FilterExperiment.USE_OPTIMIZED)));
            long legacy = time(() -> cfs1.getColumnFamily(queryFilterAll1, Optional.of(FilterExperiment.USE_LEGACY)));
            System.out.println("Optimized Run " + (i + 1) + ": " + optimized / 1_000_000 + " ms");
            System.out.println("Legacy Run " + (i + 1) + ": " + legacy / 1_000_000 + " ms");
            System.out.println("Legacy - Optimized " + (i + 1) + ": " + (legacy - optimized) / 1_000_000 + " ms");
        }
    }

//    @Test
//    public void testMicroBenchmarkLegacy()
//    {
//        List<Long> times = new ArrayList<>();
//        for (int i = 0; i < EXPERIMENTS; i++)
//        {
//            QueryFilter queryFilterAll1 = createQueryFilter(
//                    Composites.EMPTY,
//                    Composites.EMPTY,
//                    COLUMN_FAMILY_1,
//                    0
//            );
//            long legacy = time(() -> cfs1.getColumnFamily(queryFilterAll1, Optional.of(FilterExperiment.USE_LEGACY)));
//            times.add(legacy);
//            System.out.println("Legacy Run " + (i + 1) + ": " + legacy + " ns");
//        }
//
//        long min = times.stream().min(Long::compare).orElse(0L);
//        long max = times.stream().max(Long::compare).orElse(0L);
//        double avg = times.stream().mapToLong(Long::longValue).average().orElse(0.0);
//
//        System.out.println("Min: " + min + " ns");
//        System.out.println("Max: " + max + " ns");
//        System.out.println("Avg: " + avg + " ns");
//    }
//
//    @Test
//    public void testMicroBenchmarkOptimized()
//    {
//        List<Long> times = new ArrayList<>();
//        for (int i = 0; i < EXPERIMENTS; i++)
//        {
//            QueryFilter queryFilterAll2 = createQueryFilter(
//                    Composites.EMPTY,
//                    Composites.EMPTY,
//                    COLUMN_FAMILY_2,
//                    0
//            );
//            long optimized = time(() -> cfs2.getColumnFamily(queryFilterAll2, Optional.of(FilterExperiment.USE_OPTIMIZED)));
//            times.add(optimized);
//            System.out.println("Optimized Run " + (i + 1) + ": " + optimized + " ns");
//        }
//
//        long min = times.stream().min(Long::compare).orElse(0L);
//        long max = times.stream().max(Long::compare).orElse(0L);
//        double avg = times.stream().mapToLong(Long::longValue).average().orElse(0.0);
//
//        System.out.println("Min: " + min + " ns");
//        System.out.println("Max: " + max + " ns");
//        System.out.println("Avg: " + avg + " ns");
//    }

    private static void putColStandard(ColumnFamilyStore cfs, DecoratedKey key, Cell col)
    {
        ColumnFamily cf = ArrayBackedSortedColumns.factory.create(cfs.keyspace.getName(), cfs.name);
        cf.addColumn(col);
        new Mutation(cfs.keyspace.getName(), key.getKey(), cf).applyUnsafe();
        cfs.forceBlockingFlush();
    }

    private static void deleteRange(ColumnFamilyStore cfs, DecoratedKey key, RangeTombstone rt)
    {
        ColumnFamily cf = ArrayBackedSortedColumns.factory.create(cfs.keyspace.getName(), cfs.name);
        cf.delete(rt);
        new Mutation(cfs.keyspace.getName(), key.getKey(), cf).applyUnsafe();
        cfs.forceBlockingFlush();
    }

    private QueryFilter createQueryFilter(Composite start, Composite finish, String columnFamily, long timestamp)
    {
        return new QueryFilter(ROW_KEY, columnFamily, new SliceQueryFilter(start, finish, false, 100), timestamp);
    }

    private long time(Supplier<ColumnFamily> supplier)
    {
        long startTime = System.nanoTime();
        ColumnFamily cf = supplier.get();
        assert cf != null;
        assert cf.getColumnCount() == 1;
        try
        {
            assert ByteBufferUtil.string(new ArrayList<>(cf.getSortedColumns()).get(0).name().toByteBuffer()).equals(String.valueOf(0));
        }
        catch (CharacterCodingException e)
        {
            throw new RuntimeException(e);
        }
        long endTime = System.nanoTime();
        return endTime - startTime;
    }
}
