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

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import org.apache.bcel.generic.FADD;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.composites.Composites;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.cassandra.Util.*;
import static org.apache.cassandra.config.CFMetaData.DEFAULT_GC_GRACE_SECONDS;
import static org.junit.Assert.*;

/**
 * This class tests resumable range scans at the local level, that is, within a node
 * The two entry points of interest are ColumnFamilyStore#getColumnFamily(QueryFilter filter) and ColumnFamilyStore#getRangeSlice(ExtendedFilter filter),
 * which are used by SliceFromReadCommand and RangeSliceCommand, respectively.
 */
public class ResumableRangeScanLocalTest
{
    public static final String KEYSPACE = "KeyspaceForTest";
    public static final String COLUMN_FAMILY = "ColumnFamilyForTest";
    public static final DecoratedKey ROW_KEY = Util.dk("row_key");
    public static final DecoratedKey ROW_KEY_2 = Util.dk("row_key2");
    public static final int WRITE_TIMESTAMP_MS = 0;
    public static final int DELETE_TIMESTAMP_MS = 1000;

    public static final CellName CELL_NAME_C0 = cellname("c0");
    public static final CellName CELL_NAME_C1 = cellname("c1");
    public static final CellName CELL_NAME_C2 = cellname("c2");
    public static final CellName CELL_NAME_C3 = cellname("c3");
    public static final CellName CELL_NAME_C4 = cellname("c4");
    public static final CellName CELL_NAME_C5 = cellname("c5");
    public static final CellName CELL_NAME_C6 = cellname("c6");

    public static final Cell CELL_C0 = column("c0", "value", WRITE_TIMESTAMP_MS);
    public static final Cell CELL_C1 = column("c1", "value", WRITE_TIMESTAMP_MS);
    public static final Cell CELL_C2 = column("c2", "value", WRITE_TIMESTAMP_MS);
    public static final Cell CELL_C3 = column("c3", "value", WRITE_TIMESTAMP_MS);
    public static final Cell CELL_C4 = column("c4", "value", WRITE_TIMESTAMP_MS);
    public static final Cell CELL_C5 = column("c5", "value", WRITE_TIMESTAMP_MS);
    public static final Cell CELL_C6 = column("c6", "value", WRITE_TIMESTAMP_MS);

    public ColumnFamilyStore cfs;

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                SimpleStrategy.class,
                KSMetaData.optsWithRF(1),
                SchemaLoader.standardCFMD(KEYSPACE, COLUMN_FAMILY));
    }

    @Before
    public void beforeEach()
    {
        cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(COLUMN_FAMILY);
        cfs.clearUnsafe();

        putColsStandard(
                cfs,
                ROW_KEY,
                CELL_C0,
                CELL_C1,
                CELL_C2,
                CELL_C3,
                CELL_C4,
                CELL_C5,
                CELL_C6
        );
        cfs.forceBlockingFlush();
    }

    @Test
    public void testGetColumnFamily_stopsAtThreshold()
    {
        QueryFilter queryFilterAll = createQueryFilter(Composites.EMPTY, Composites.EMPTY, 0);
        ColumnFamily cf = cfs.getColumnFamily(queryFilterAll);

        assertCellsAndPageToken(cf, ImmutableList.of(CELL_NAME_C0, CELL_NAME_C1, CELL_NAME_C2, CELL_NAME_C3), PageToken.createPageToken(CELL_C4));
    }

    @Test
    public void testGetColumnFamily_withStartSet()
    {
        QueryFilter queryFilterC1 = createQueryFilter(CELL_NAME_C1, Composites.EMPTY, 0);
        ColumnFamily cf = cfs.getColumnFamily(queryFilterC1);

        assertCellsAndPageToken(cf, ImmutableList.of(CELL_NAME_C1, CELL_NAME_C2, CELL_NAME_C3, CELL_NAME_C4), PageToken.createPageToken(CELL_C5));
    }

    @Test
    public void testGetColumnFamily_withFinishSet()
    {
        QueryFilter queryFilterC5 = createQueryFilter(Composites.EMPTY, CELL_NAME_C5, 0);
        ColumnFamily cf = cfs.getColumnFamily(queryFilterC5);

        assertCellsAndPageToken(cf, ImmutableList.of(CELL_NAME_C0, CELL_NAME_C1, CELL_NAME_C2, CELL_NAME_C3), PageToken.createPageToken(CELL_C4));
    }

    @Test
    public void testGetColumnFamily_withStartAndFinishSet()
    {
        QueryFilter queryFilterC1C5 = createQueryFilter(CELL_NAME_C1, CELL_NAME_C5, 0);
        ColumnFamily cf = cfs.getColumnFamily(queryFilterC1C5);

        assertCellsAndPageToken(cf, ImmutableList.of(CELL_NAME_C1, CELL_NAME_C2, CELL_NAME_C3, CELL_NAME_C4), PageToken.createPageToken(CELL_C5));
    }

    @Test
    public void testGetColumnFamily_withMultipleSlices()
    {
        QueryFilter queryFilterC1C2C4C6 = createQueryFilter(CELL_NAME_C1, CELL_NAME_C2, CELL_NAME_C4, CELL_NAME_C6, 0);
        ColumnFamily cf = cfs.getColumnFamily(queryFilterC1C2C4C6);

        assertCellsAndPageToken(cf, ImmutableList.of(CELL_NAME_C1, CELL_NAME_C2, CELL_NAME_C4, CELL_NAME_C5), PageToken.createPageToken(CELL_C6));
    }

    @Test
    public void testGetColumnFamily_returnsEndOfRowPageToken()
    {
        QueryFilter queryFilterC4 = createQueryFilter(CELL_NAME_C4, Composites.EMPTY, 0);
        ColumnFamily cf = cfs.getColumnFamily(queryFilterC4);

        assertCellsAndPageToken(cf, ImmutableList.of(CELL_NAME_C4, CELL_NAME_C5, CELL_NAME_C6), PageToken.createPageTokenReachedEnd());
    }

    @Test
    public void testGetColumnFamily_returnsValuedPageTokenWhenLastCellHitsThreshold()
    {
        QueryFilter queryFilterC2 = createQueryFilter(CELL_NAME_C2, Composites.EMPTY, 0);
        ColumnFamily cf = cfs.getColumnFamily(queryFilterC2);

        assertCellsAndPageToken(cf, ImmutableList.of(CELL_NAME_C2, CELL_NAME_C3, CELL_NAME_C4, CELL_NAME_C5), PageToken.createPageToken(CELL_C6));
    }

    @Test
    public void testGetColumnFamily_withSetRangeReturnsValuedPageTokenWhenLastCellHitsThreshold()
    {
        QueryFilter queryFilterC2 = createQueryFilter(CELL_NAME_C2, CELL_NAME_C6, 0);
        ColumnFamily cf = cfs.getColumnFamily(queryFilterC2);

        assertCellsAndPageToken(cf, ImmutableList.of(CELL_NAME_C2, CELL_NAME_C3, CELL_NAME_C4, CELL_NAME_C5), PageToken.createPageToken(CELL_C6));
    }

    @Test
    public void testGetColumnFamily_returnsEndOfRowPageTokenWhenLastCellAlmostHitsThreshold()
    {
        QueryFilter queryFilterC3 = createQueryFilter(CELL_NAME_C3, Composites.EMPTY, 0);
        ColumnFamily cf = cfs.getColumnFamily(queryFilterC3);

        assertCellsAndPageToken(cf, ImmutableList.of(CELL_NAME_C3, CELL_NAME_C4, CELL_NAME_C5, CELL_NAME_C6), PageToken.createPageTokenReachedEnd());
    }

    @Test
    public void testGetColumnFamily_withSetRangeReturnsEndOfRowPageTokenWhenLastCellAlmostHitsThreshold()
    {
        QueryFilter queryFilterC1C4 = createQueryFilter(CELL_NAME_C1, CELL_NAME_C4, 0);
        ColumnFamily cf = cfs.getColumnFamily(queryFilterC1C4);

        assertCellsAndPageToken(cf, ImmutableList.of(CELL_NAME_C1, CELL_NAME_C2, CELL_NAME_C3, CELL_NAME_C4), PageToken.createPageTokenReachedEnd());
    }

    @Test
    public void testGetColumnFamily_withMultipleSlicesReturnsEndOfRowPageToken()
    {
        QueryFilter queryFilterC1C2C4C6 = createQueryFilter(CELL_NAME_C1, CELL_NAME_C2, CELL_NAME_C4, CELL_NAME_C5, 0);
        ColumnFamily cf = cfs.getColumnFamily(queryFilterC1C2C4C6);

        assertCellsAndPageToken(cf, ImmutableList.of(CELL_NAME_C1, CELL_NAME_C2, CELL_NAME_C4, CELL_NAME_C5), PageToken.createPageTokenReachedEnd());
    }

    @Test
    public void testGetColumnFamily_returnsValuedPageTokenWhenAllTombstoned()
    {
        deleteRange(cfs, ROW_KEY, tombstone("c0", "c6", DELETE_TIMESTAMP_MS, DELETE_TIMESTAMP_MS));

        QueryFilter queryFilterAll = createQueryFilter(Composites.EMPTY, Composites.EMPTY, 0);
        ColumnFamily cf = cfs.getColumnFamily(queryFilterAll);

        assertCellsAndPageToken(cf, ImmutableList.of(), PageToken.createPageToken(CELL_C4));
    }

    @Test
    public void testGetColumnFamily_returnsValuedPageTokenWhenPartiallyTombstoned()
    {
        deleteRange(cfs, ROW_KEY, tombstone("c0", "c2", DELETE_TIMESTAMP_MS, DELETE_TIMESTAMP_MS));

        QueryFilter queryFilterAll = createQueryFilter(Composites.EMPTY, Composites.EMPTY, 0);
        ColumnFamily cf = cfs.getColumnFamily(queryFilterAll);

        assertCellsAndPageToken(cf, ImmutableList.of(CELL_NAME_C3), PageToken.createPageToken(CELL_C4));
    }

    @Test
    public void testGetColumnFamily_returnsNonNullWhenValuedPageTokenSet()
    {
        deleteRange(cfs, ROW_KEY, tombstone("c0", "c6", DELETE_TIMESTAMP_MS, DELETE_TIMESTAMP_MS));

        QueryFilter queryFilterAllFarFuture = createQueryFilter(Composites.EMPTY, Composites.EMPTY,
                DELETE_TIMESTAMP_MS + 2 * DEFAULT_GC_GRACE_SECONDS * 1000);
        ColumnFamily cf = cfs.getColumnFamily(queryFilterAllFarFuture);

        assertNotNull(cf);
        assertFalse(cf.hasColumns());
        assertCellsAndPageToken(cf, Collections.emptyList(), PageToken.createPageToken(CELL_C4));
    }

    @Test
    public void testGetColumnFamily_returnsNonNullWhenEndOfRowPageTokenSet()
    {
        deleteRange(cfs, ROW_KEY, tombstone("c0", "c6", DELETE_TIMESTAMP_MS, DELETE_TIMESTAMP_MS));

        QueryFilter queryFilterAllFarFuture = createQueryFilter(CELL_NAME_C6, Composites.EMPTY,
                DELETE_TIMESTAMP_MS + 2 * DEFAULT_GC_GRACE_SECONDS * 1000);
        ColumnFamily cf = cfs.getColumnFamily(queryFilterAllFarFuture);

        assertNotNull(cf);
        assertFalse(cf.hasColumns());
        assertCellsAndPageToken(cf, Collections.emptyList(), PageToken.createPageTokenReachedEnd());
    }

    @Test
    public void testGetColumnFamily_returnsPageTokenWhenNoData()
    {
        QueryFilter queryFilterAllFarFuture = createQueryFilter(Composites.EMPTY, Composites.EMPTY, 0, ROW_KEY_2);
        ColumnFamily cf = cfs.getColumnFamily(queryFilterAllFarFuture);

        assertNotNull(cf);
        assertFalse(cf.hasColumns());
        assertCellsAndPageToken(cf, Collections.emptyList(), PageToken.createPageTokenReachedEnd());
    }

    @Test
    public void testGetColumnFamily_correctlyReconcilesDuplicateCells()
    {
        putColsStandard(cfs, ROW_KEY, column("c0", "value", WRITE_TIMESTAMP_MS + 1000), column("c1", "value", WRITE_TIMESTAMP_MS + 1000), column("c2", "value"
                , WRITE_TIMESTAMP_MS + 1000));

        QueryFilter queryFilterAll = createQueryFilter(Composites.EMPTY, Composites.EMPTY, 0);
        ColumnFamily cf = cfs.getColumnFamily(queryFilterAll);

        assertCellsAndPageToken(cf, ImmutableList.of(CELL_NAME_C0, CELL_NAME_C1, CELL_NAME_C2, CELL_NAME_C3), PageToken.createPageToken(CELL_C4));
    }

    @Test
    public void testGetColumnFamily_correctlyReconcilesOverlappingTombstones()
    {
        deleteRange(cfs, ROW_KEY, tombstone("c0", "c2", DELETE_TIMESTAMP_MS + 1000, DELETE_TIMESTAMP_MS + 1000), tombstone("c1", "c3",
                DELETE_TIMESTAMP_MS + 1000, DELETE_TIMESTAMP_MS + 1000));

        QueryFilter queryFilterAll = createQueryFilter(Composites.EMPTY, Composites.EMPTY, 0);
        ColumnFamily cf = cfs.getColumnFamily(queryFilterAll);

        assertCellsAndPageToken(cf, ImmutableList.of(), PageToken.createPageToken(CELL_C4));
    }

    @Test
    public void testGetColumnFamily_handlesPointTombstones()
    {
        Cell tombstoneC0 = expiredColumn("c0", "value", DELETE_TIMESTAMP_MS + 1000);
        Cell tombstoneC1 = expiredColumn("c1", "value", DELETE_TIMESTAMP_MS + 1000);
        putColsStandard(cfs, ROW_KEY, tombstoneC0, tombstoneC1);
        cfs.forceBlockingFlush();

        QueryFilter queryFilterAll = createQueryFilter(Composites.EMPTY, Composites.EMPTY, 0);
        ColumnFamily cf = cfs.getColumnFamily(queryFilterAll);

        assertCellsAndPageToken(cf, ImmutableList.of(tombstoneC0.name(), tombstoneC1.name(), CELL_NAME_C2, CELL_NAME_C3), PageToken.createPageToken(CELL_C4));
    }

    @Test
    public void testGetColumnFamily_returnsPointTombstoneValuedPageToken()
    {
        Cell tombstoneC0 = expiredColumn("c0", "value", DELETE_TIMESTAMP_MS + 1000);
        Cell tombstoneC1 = expiredColumn("c1", "value", DELETE_TIMESTAMP_MS + 1000);
        Cell tombstoneC4 = expiredColumn("c4", "value", DELETE_TIMESTAMP_MS + 1000);
        putColsStandard(cfs, ROW_KEY, tombstoneC0, tombstoneC1, tombstoneC4);
        cfs.forceBlockingFlush();

        QueryFilter queryFilterAll = createQueryFilter(Composites.EMPTY, Composites.EMPTY, 0);
        ColumnFamily cf = cfs.getColumnFamily(queryFilterAll);

        assertCellsAndPageToken(cf, ImmutableList.of(tombstoneC0.name(), tombstoneC1.name(), CELL_NAME_C2, CELL_NAME_C3),
                PageToken.createPageToken(tombstoneC4));
    }


    @Test
    public void testGetRangeSlice_stopsAtThreshold()
    {
        ExtendedFilter extendedFilterAll = createExtendedFilter(Composites.EMPTY, Composites.EMPTY, 0);
        ColumnFamily cf = cfs.getRangeSlice(extendedFilterAll).get(0).cf;

        assertCellsAndPageToken(cf, ImmutableList.of(CELL_NAME_C0, CELL_NAME_C1, CELL_NAME_C2, CELL_NAME_C3), PageToken.createPageToken(CELL_C4));
    }

    @Test
    public void testGetRangeSlice_withStartSet()
    {
        ExtendedFilter extendedFilterC1 = createExtendedFilter(CELL_NAME_C1, Composites.EMPTY, 0);
        ColumnFamily cf = cfs.getRangeSlice(extendedFilterC1).get(0).cf;

        assertCellsAndPageToken(cf, ImmutableList.of(CELL_NAME_C1, CELL_NAME_C2, CELL_NAME_C3, CELL_NAME_C4), PageToken.createPageToken(CELL_C5));
    }

    @Test
    public void testGetRangeSlice_withFinishSet()
    {
        ExtendedFilter extendedFilterC5 = createExtendedFilter(Composites.EMPTY, CELL_NAME_C5, 0);
        ColumnFamily cf = cfs.getRangeSlice(extendedFilterC5).get(0).cf;

        assertCellsAndPageToken(cf, ImmutableList.of(CELL_NAME_C0, CELL_NAME_C1, CELL_NAME_C2, CELL_NAME_C3), PageToken.createPageToken(CELL_C4));
    }

    @Test
    public void testGetRangeSlice_withStartAndFinishSet()
    {
        ExtendedFilter extendedFilterC1C5 = createExtendedFilter(CELL_NAME_C1, CELL_NAME_C5, 0);
        ColumnFamily cf = cfs.getRangeSlice(extendedFilterC1C5).get(0).cf;

        assertCellsAndPageToken(cf, ImmutableList.of(CELL_NAME_C1, CELL_NAME_C2, CELL_NAME_C3, CELL_NAME_C4), PageToken.createPageToken(CELL_C5));
    }

    @Test
    public void testGetRangeSlice_withMultipleSlices()
    {
        ExtendedFilter extendedFilterC1C2C4C6 = createExtendedFilter(CELL_NAME_C1, CELL_NAME_C2, CELL_NAME_C4, CELL_NAME_C6, 0);
        ColumnFamily cf = cfs.getRangeSlice(extendedFilterC1C2C4C6).get(0).cf;

        assertCellsAndPageToken(cf, ImmutableList.of(CELL_NAME_C1, CELL_NAME_C2, CELL_NAME_C4, CELL_NAME_C5), PageToken.createPageToken(CELL_C6));
    }

    @Test
    public void testGetRangeSlice_returnsEndOfRowPageToken()
    {
        ExtendedFilter extendedFilterC4 = createExtendedFilter(CELL_NAME_C4, Composites.EMPTY, 0);
        ColumnFamily cf = cfs.getRangeSlice(extendedFilterC4).get(0).cf;

        assertCellsAndPageToken(cf, ImmutableList.of(CELL_NAME_C4, CELL_NAME_C5, CELL_NAME_C6), PageToken.createPageTokenReachedEnd());
    }

    @Test
    public void testGetRangeSlice_returnsValuedPageTokenWhenLastCellHitsThreshold()
    {
        ExtendedFilter extendedFilterC2 = createExtendedFilter(CELL_NAME_C2, Composites.EMPTY, 0);
        ColumnFamily cf = cfs.getRangeSlice(extendedFilterC2).get(0).cf;

        assertCellsAndPageToken(cf, ImmutableList.of(CELL_NAME_C2, CELL_NAME_C3, CELL_NAME_C4, CELL_NAME_C5), PageToken.createPageToken(CELL_C6));
    }

    @Test
    public void testGetRangeSlice_withSetRangeReturnsValuedPageTokenWhenLastCellHitsThreshold()
    {
        ExtendedFilter extendedFilterC2 = createExtendedFilter(CELL_NAME_C2, CELL_NAME_C6, 0);
        ColumnFamily cf = cfs.getRangeSlice(extendedFilterC2).get(0).cf;

        assertCellsAndPageToken(cf, ImmutableList.of(CELL_NAME_C2, CELL_NAME_C3, CELL_NAME_C4, CELL_NAME_C5), PageToken.createPageToken(CELL_C6));
    }

    @Test
    public void testGetRangeSlice_returnsEndOfRowPageTokenWhenLastCellAlmostHitsThreshold()
    {
        ExtendedFilter extendedFilterC3 = createExtendedFilter(CELL_NAME_C3, Composites.EMPTY, 0);
        ColumnFamily cf = cfs.getRangeSlice(extendedFilterC3).get(0).cf;

        assertCellsAndPageToken(cf, ImmutableList.of(CELL_NAME_C3, CELL_NAME_C4, CELL_NAME_C5, CELL_NAME_C6), PageToken.createPageTokenReachedEnd());
    }

    @Test
    public void testGetRangeSlice_withSetRangeReturnsEndOfRowPageTokenWhenLastCellAlmostHitsThreshold()
    {
        ExtendedFilter extendedFilterC1C4 = createExtendedFilter(CELL_NAME_C1, CELL_NAME_C4, 0);
        ColumnFamily cf = cfs.getRangeSlice(extendedFilterC1C4).get(0).cf;

        assertCellsAndPageToken(cf, ImmutableList.of(CELL_NAME_C1, CELL_NAME_C2, CELL_NAME_C3, CELL_NAME_C4), PageToken.createPageTokenReachedEnd());
    }

    @Test
    public void testGetRangeSlice_withMultipleSlicesReturnsEndOfRowPageToken()
    {
        ExtendedFilter extendedFilterC1C2C4C6 = createExtendedFilter(CELL_NAME_C1, CELL_NAME_C2, CELL_NAME_C4, CELL_NAME_C5, 0);
        ColumnFamily cf = cfs.getRangeSlice(extendedFilterC1C2C4C6).get(0).cf;

        assertCellsAndPageToken(cf, ImmutableList.of(CELL_NAME_C1, CELL_NAME_C2, CELL_NAME_C4, CELL_NAME_C5), PageToken.createPageTokenReachedEnd());
    }

    @Test
    public void testGetRangeSlice_returnsValuedPageTokenWhenAllTombstoned()
    {
        deleteRange(cfs, ROW_KEY, tombstone("c0", "c6", DELETE_TIMESTAMP_MS, DELETE_TIMESTAMP_MS));

        ExtendedFilter extendedFilterAll = createExtendedFilter(Composites.EMPTY, Composites.EMPTY, 0);
        ColumnFamily cf = cfs.getRangeSlice(extendedFilterAll).get(0).cf;

        assertCellsAndPageToken(cf, ImmutableList.of(), PageToken.createPageToken(CELL_C4));
    }

    @Test
    public void testGetRangeSlice_returnsValuedPageTokenWhenPartiallyTombstoned()
    {
        deleteRange(cfs, ROW_KEY, tombstone("c0", "c2", DELETE_TIMESTAMP_MS, DELETE_TIMESTAMP_MS));

        ExtendedFilter extendedFilterAll = createExtendedFilter(Composites.EMPTY, Composites.EMPTY, 0);
        ColumnFamily cf = cfs.getRangeSlice(extendedFilterAll).get(0).cf;

        assertCellsAndPageToken(cf, ImmutableList.of(CELL_NAME_C3), PageToken.createPageToken(CELL_C4));
    }

    @Test
    public void testGetRangeSlice_returnsNonNullWhenValuedPageTokenSet()
    {
        deleteRange(cfs, ROW_KEY, tombstone("c0", "c6", DELETE_TIMESTAMP_MS, DELETE_TIMESTAMP_MS));

        ExtendedFilter extendedFilterAllFarFuture = createExtendedFilter(Composites.EMPTY, Composites.EMPTY,
                DELETE_TIMESTAMP_MS + 2 * DEFAULT_GC_GRACE_SECONDS * 1000);
        ColumnFamily cf = cfs.getRangeSlice(extendedFilterAllFarFuture).get(0).cf;

        assertNotNull(cf);
        assertFalse(cf.hasColumns());
        assertCellsAndPageToken(cf, Collections.emptyList(), PageToken.createPageToken(CELL_C4));
    }

    @Test
    public void testGetRangeSlice_returnsNonNullWhenEndOfRowPageTokenSet()
    {
        deleteRange(cfs, ROW_KEY, tombstone("c0", "c6", DELETE_TIMESTAMP_MS, DELETE_TIMESTAMP_MS));

        ExtendedFilter extendedFilterAllFarFuture = createExtendedFilter(CELL_NAME_C6, Composites.EMPTY,
                DELETE_TIMESTAMP_MS + 2 * DEFAULT_GC_GRACE_SECONDS * 1000);
        ColumnFamily cf = cfs.getRangeSlice(extendedFilterAllFarFuture).get(0).cf;

        assertNotNull(cf);
        assertFalse(cf.hasColumns());
        assertCellsAndPageToken(cf, Collections.emptyList(), PageToken.createPageTokenReachedEnd());
    }

    @Test
    public void testGetRangeSlice_returnsPageTokenWhenNoData()
    {
        ExtendedFilter extendedFilterAllFarFuture = createExtendedFilter(Composites.EMPTY, Composites.EMPTY, 0, ROW_KEY_2);
        ColumnFamily cf = cfs.getRangeSlice(extendedFilterAllFarFuture).get(0).cf;

        assertNotNull(cf);
        assertFalse(cf.hasColumns());
        assertCellsAndPageToken(cf, Collections.emptyList(), PageToken.createPageTokenReachedEnd());
    }

    @Test
    public void testGetRangeSlice_correctlyReconcilesDuplicateCells()
    {
        putColsStandard(cfs, ROW_KEY, column("c0", "value", WRITE_TIMESTAMP_MS + 1000), column("c1", "value", WRITE_TIMESTAMP_MS + 1000), column("c2", "value"
                , WRITE_TIMESTAMP_MS + 1000));

        ExtendedFilter extendedFilterAll = createExtendedFilter(Composites.EMPTY, Composites.EMPTY, 0);
        ColumnFamily cf = cfs.getRangeSlice(extendedFilterAll).get(0).cf;

        assertCellsAndPageToken(cf, ImmutableList.of(CELL_NAME_C0, CELL_NAME_C1, CELL_NAME_C2, CELL_NAME_C3), PageToken.createPageToken(CELL_C4));
    }

    @Test
    public void testGetRangeSlice_correctlyReconcilesOverlappingTombstones()
    {
        deleteRange(cfs, ROW_KEY, tombstone("c0", "c2", DELETE_TIMESTAMP_MS + 1000, DELETE_TIMESTAMP_MS + 1000), tombstone("c1", "c3",
                DELETE_TIMESTAMP_MS + 1000, DELETE_TIMESTAMP_MS + 1000));

        ExtendedFilter extendedFilterAll = createExtendedFilter(Composites.EMPTY, Composites.EMPTY, 0);
        ColumnFamily cf = cfs.getRangeSlice(extendedFilterAll).get(0).cf;

        assertCellsAndPageToken(cf, ImmutableList.of(), PageToken.createPageToken(CELL_C4));
    }

    @Test
    public void testGetRangeSlice_handlesPointTombstones()
    {
        Cell tombstoneC0 = expiredColumn("c0", "value", DELETE_TIMESTAMP_MS + 1000);
        Cell tombstoneC1 = expiredColumn("c1", "value", DELETE_TIMESTAMP_MS + 1000);
        putColsStandard(cfs, ROW_KEY, tombstoneC0, tombstoneC1);
        cfs.forceBlockingFlush();

        ExtendedFilter extendedFilterAll = createExtendedFilter(Composites.EMPTY, Composites.EMPTY, 0);
        ColumnFamily cf = cfs.getRangeSlice(extendedFilterAll).get(0).cf;

        assertCellsAndPageToken(cf, ImmutableList.of(tombstoneC0.name(), tombstoneC1.name(), CELL_NAME_C2, CELL_NAME_C3), PageToken.createPageToken(CELL_C4));
    }

    @Test
    public void testGetRangeSlice_returnsPointTombstoneValuedPageToken()
    {
        Cell tombstoneC0 = expiredColumn("c0", "value", DELETE_TIMESTAMP_MS + 1000);
        Cell tombstoneC1 = expiredColumn("c1", "value", DELETE_TIMESTAMP_MS + 1000);
        Cell tombstoneC4 = expiredColumn("c4", "value", DELETE_TIMESTAMP_MS + 1000);
        putColsStandard(cfs, ROW_KEY, tombstoneC0, tombstoneC1, tombstoneC4);
        cfs.forceBlockingFlush();

        ExtendedFilter extendedFilterAll = createExtendedFilter(Composites.EMPTY, Composites.EMPTY, 0);
        ColumnFamily cf = cfs.getRangeSlice(extendedFilterAll).get(0).cf;

        assertCellsAndPageToken(cf, ImmutableList.of(tombstoneC0.name(), tombstoneC1.name(), CELL_NAME_C2, CELL_NAME_C3),
                PageToken.createPageToken(tombstoneC4));
    }

    @Test
    public void testGetRangeSlice_handlesMultipleRowsIndependently()
    {
        putColsStandard(
                cfs,
                ROW_KEY_2,
                CELL_C1,
                CELL_C2,
                CELL_C3,
                CELL_C4,
                CELL_C5,
                CELL_C6
        );
        cfs.forceBlockingFlush();

        ExtendedFilter extendedFilterAll = createExtendedFilter(Composites.EMPTY, Composites.EMPTY, 0);
        List<Row> rows = cfs.getRangeSlice(extendedFilterAll);

        assertCellsAndPageToken(rows.get(0).cf, ImmutableList.of(CELL_NAME_C0, CELL_NAME_C1, CELL_NAME_C2, CELL_NAME_C3), PageToken.createPageToken(CELL_C4));
        assertCellsAndPageToken(rows.get(1).cf, ImmutableList.of(CELL_NAME_C1, CELL_NAME_C2, CELL_NAME_C3, CELL_NAME_C4), PageToken.createPageToken(CELL_C5));
    }

    private static void putColsStandard(ColumnFamilyStore cfs, DecoratedKey key, Cell... cols)
    {
        ColumnFamily cf = ArrayBackedSortedColumns.factory.create(cfs.keyspace.getName(), cfs.name);
        for (Cell col : cols)
        {
            cf.addColumn(col);
        }
        Mutation rm = new Mutation(cfs.keyspace.getName(), key.getKey(), cf);
        rm.applyUnsafe();
    }

    private static void deleteRange(ColumnFamilyStore cfs, DecoratedKey key, RangeTombstone... rangeTombstones)
    {
        ColumnFamily cf = ArrayBackedSortedColumns.factory.create(cfs.keyspace.getName(), cfs.name);
        for (RangeTombstone rangeTombstone : rangeTombstones)
        {
            cf.delete(rangeTombstone);
        }
        Mutation rm = new Mutation(cfs.keyspace.getName(), key.getKey(), cf);
        rm.applyUnsafe();
        cfs.forceBlockingFlush();
    }

    private QueryFilter createQueryFilter(Composite start, Composite finish, long timestamp)
    {
        return new QueryFilter(ROW_KEY, COLUMN_FAMILY, new SliceQueryFilter(start, finish, false, true, 100), timestamp);
    }

    private QueryFilter createQueryFilter(Composite start, Composite finish, long timestamp, DecoratedKey decoratedKey)
    {
        return new QueryFilter(decoratedKey, COLUMN_FAMILY, new SliceQueryFilter(start, finish, false, true, 100), timestamp);
    }

    private QueryFilter createQueryFilter(Composite start1, Composite finish1, Composite start2, Composite finish2, long timestamp)
    {
        return new QueryFilter(ROW_KEY, COLUMN_FAMILY, new SliceQueryFilter(new ColumnSlice[]{
                new ColumnSlice(start1, finish1), new ColumnSlice(start2,
                finish2)}, false, true, 100), timestamp);
    }

    private ExtendedFilter createExtendedFilter(Composite start, Composite finish, long timestamp)
    {
        SliceQueryFilter filter = new SliceQueryFilter(start, finish, false, true, 100);
        DataRange dataRange = new DataRange(Bounds.makeRowBounds(ROW_KEY.getToken(), ROW_KEY_2.getToken()), filter);

        return ExtendedFilter.create(cfs, dataRange, ImmutableList.of(), 100, false, timestamp);
    }

    private ExtendedFilter createExtendedFilter(Composite start, Composite finish, long timestamp, DecoratedKey decoratedKey)
    {
        SliceQueryFilter filter = new SliceQueryFilter(start, finish, false, true, 100);
        DataRange dataRange = new DataRange(Bounds.makeRowBounds(decoratedKey.getToken(), decoratedKey.getToken()), filter);

        return ExtendedFilter.create(cfs, dataRange, ImmutableList.of(), 100, false, timestamp);
    }

    private ExtendedFilter createExtendedFilter(Composite start1, Composite finish1, Composite start2, Composite finish2, long timestamp)
    {
        SliceQueryFilter filter = new SliceQueryFilter(new ColumnSlice[]{new ColumnSlice(start1, finish1), new ColumnSlice(start2, finish2)}, false, true, 100);
        DataRange dataRange = new DataRange(Bounds.makeRowBounds(ROW_KEY.getToken(), ROW_KEY_2.getToken()), filter);

        return ExtendedFilter.create(cfs, dataRange, ImmutableList.of(), 100, false, timestamp);
    }

    private static void assertCellsAndPageToken(ColumnFamily cf, Collection<CellName> expectedCells, PageToken expectedPageToken)
    {
        assertNotNull(cf);
        assertEquals(new HashSet<>(expectedCells), cf.getSortedColumns().stream().map(Cell::name).collect(Collectors.toSet()));
        assertEquals(cf.pageToken(), expectedPageToken);
    }
}
