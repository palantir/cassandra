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
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.composites.Composites;
import org.apache.cassandra.db.filter.PageToken;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

import static org.apache.cassandra.Util.*;
import static org.apache.cassandra.config.CFMetaData.DEFAULT_GC_GRACE_SECONDS;
import static org.junit.Assert.*;

/**
 * This class tests resumable range scans at the local level, that is within a node
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

    public static final Cell CELL_C0 = column("c0", "value", WRITE_TIMESTAMP_MS);
    public static final Cell CELL_C1 = column("c1", "value", WRITE_TIMESTAMP_MS);
    public static final Cell CELL_C2 = column("c2", "value", WRITE_TIMESTAMP_MS);
    public static final Cell CELL_C3 = column("c3", "value", WRITE_TIMESTAMP_MS);
    public static final Cell CELL_C4 = column("c4", "value", WRITE_TIMESTAMP_MS);
    public static final Cell CELL_C5 = column("c5", "value", WRITE_TIMESTAMP_MS);
    public static final Cell CELL_C6 = column("c6", "value", WRITE_TIMESTAMP_MS);
    public static final RangeTombstone TOMBSTONE_C0_C4 = tombstone("c0", "c4", DELETE_TIMESTAMP_MS, DELETE_TIMESTAMP_MS);

    public ColumnFamilyStore cfs;
    public QueryFilter queryFilterAll;
    public QueryFilter queryFilterAllExpiredTombstones;

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
        putColsStandard(
                cfs,
                ROW_KEY_2,
                CELL_C0,
                CELL_C1,
                CELL_C2,
                CELL_C3,
                CELL_C4,
                CELL_C5,
                CELL_C6
        );
        cfs.forceBlockingFlush();

        deleteRange(cfs, ROW_KEY_2, TOMBSTONE_C0_C4);
        cfs.forceBlockingFlush();

        queryFilterAll = new QueryFilter(ROW_KEY, COLUMN_FAMILY, new SliceQueryFilter(Composites.EMPTY, Composites.EMPTY, false, true, 100), 0);
        queryFilterAllExpiredTombstones = new QueryFilter(ROW_KEY_2, COLUMN_FAMILY, new SliceQueryFilter(Composites.EMPTY, Composites.EMPTY, false, true, 100),
                DELETE_TIMESTAMP_MS + 2 * DEFAULT_GC_GRACE_SECONDS * 1000);
    }

    @Test
    public void testGetColumnFamily_returnsNonNullWhenPageTokenSet()
    {
        ColumnFamily cf = cfs.getColumnFamily(queryFilterAllExpiredTombstones);
        assertNotNull(cf);
        assertFalse(cf.hasColumns());
        assertFalse(cf.isMarkedForDelete());
        assertCellsAndPageToken(cf, Collections.emptyList(), PageToken.createPageToken(CELL_C4));
    }

//    @Test
//    public void testRangeSlicePageTokenVariousSlices()
//    {
//        SlicePredicate spAll = new SlicePredicate();
//        spAll.setSlice_range(new SliceRange());
//        spAll.getSlice_range().setCount(1);
//        spAll.getSlice_range().setStart(ArrayUtils.EMPTY_BYTE_ARRAY);
//        spAll.getSlice_range().setFinish(ArrayUtils.EMPTY_BYTE_ARRAY);
//
//        SlicePredicate spStartC1 = new SlicePredicate();
//        spStartC1.setSlice_range(new SliceRange());
//        spStartC1.getSlice_range().setCount(1);
//        spStartC1.getSlice_range().setStart(ByteBufferUtil.bytes("c1"));
//        spStartC1.getSlice_range().setFinish(ArrayUtils.EMPTY_BYTE_ARRAY);
//
//        SlicePredicate spEndC4 = new SlicePredicate().setSlice_range(new SliceRange().setCount(1));
//        spEndC4.setSlice_range(new SliceRange());
//        spEndC4.getSlice_range().setCount(1);
//        spEndC4.getSlice_range().setStart(ArrayUtils.EMPTY_BYTE_ARRAY);
//        spEndC4.getSlice_range().setFinish(ByteBufferUtil.bytes("c4"));
//
//        SlicePredicate spStartC1EndC4 = new SlicePredicate();
//        spStartC1EndC4.setSlice_range(new SliceRange());
//        spStartC1EndC4.getSlice_range().setCount(1);
//        spStartC1EndC4.getSlice_range().setStart(ByteBufferUtil.bytes("c1"));
//        spStartC1EndC4.getSlice_range().setFinish(ByteBufferUtil.bytes("c4"));
//
//        PageToken pageToken4 = PageToken.createPageToken(cols[4]);
//        PageToken pageToken5 = PageToken.createPageToken(cols[5]);
//        PageToken pageTokenEnd = PageToken.createPageTokenReachedEnd();
//
//        // rows: all ranges, columns: all ranges
//        assertTotalColCountAndPageTokens(cfs.getRangeSlice(Util.range("", ""),
//                        null,
//                        ThriftValidation.asIFilterUsingPageToken(spAll, cfs.metadata, null),
//                        100,
//                        System.currentTimeMillis(),
//                        true,
//                        false),
//                19,
//                ImmutableList.of(pageToken4, pageToken4, pageToken4, pageTokenEnd, pageTokenEnd));
//        // rows: (C, E], columns: all ranges
//        assertTotalColCountAndPageTokens(cfs.getRangeSlice(Util.range("C", "E"),
//                        null,
//                        ThriftValidation.asIFilterUsingPageToken(spAll, cfs.metadata, null),
//                        100,
//                        System.currentTimeMillis(),
//                        true,
//                        false),
//                7,
//                ImmutableList.of(pageTokenEnd, pageTokenEnd));
//        // rows: all ranges, columns: [c1, \inf)
//        assertTotalColCountAndPageTokens(cfs.getRangeSlice(Util.range("", ""),
//                        null,
//                        ThriftValidation.asIFilterUsingPageToken(spStartC1, cfs.metadata, null),
//                        100,
//                        System.currentTimeMillis(),
//                        true,
//                        false),
//                17,
//                ImmutableList.of(pageToken5, pageToken5, pageTokenEnd, pageTokenEnd, pageTokenEnd));
//        // rows: (C, E], columns: [c1, \inf)
//        assertTotalColCountAndPageTokens(cfs.getRangeSlice(Util.range("C", "E"),
//                        null,
//                        ThriftValidation.asIFilterUsingPageToken(spStartC1, cfs.metadata, null),
//                        100,
//                        System.currentTimeMillis(),
//                        true,
//                        false),
//                5,
//                ImmutableList.of(pageTokenEnd, pageTokenEnd));
//        // rows: all ranges, columns: (-\inf, c4]
//        assertTotalColCountAndPageTokens(cfs.getRangeSlice(Util.range("", ""),
//                        null,
//                        ThriftValidation.asIFilterUsingPageToken(spEndC4, cfs.metadata, null),
//                        100,
//                        System.currentTimeMillis(),
//                        true,
//                        false),
//                19,
//                ImmutableList.of(pageToken4, pageToken4, pageToken4, pageTokenEnd, pageTokenEnd));
//        // rows: (C, E], columns: (-\inf, c4]
//        assertTotalColCountAndPageTokens(cfs.getRangeSlice(Util.range("C", "E"),
//                        null,
//                        ThriftValidation.asIFilterUsingPageToken(spEndC4, cfs.metadata, null),
//                        100,
//                        System.currentTimeMillis(),
//                        true,
//                        false),
//                7,
//                ImmutableList.of(pageTokenEnd, pageTokenEnd));
//        // rows: all ranges, columns: [c1, c4]
//        assertTotalColCountAndPageTokens(cfs.getRangeSlice(Util.range("", ""),
//                        null,
//                        ThriftValidation.asIFilterUsingPageToken(spStartC1EndC4, cfs.metadata, null),
//                        100,
//                        System.currentTimeMillis(),
//                        true,
//                        false),
//                17,
//                ImmutableList.of(pageTokenEnd, pageTokenEnd, pageTokenEnd, pageTokenEnd, pageTokenEnd));
//        // rows: (C, E], columns: [c1, c4]
//        assertTotalColCountAndPageTokens(cfs.getRangeSlice(Util.range("C", "E"),
//                        null,
//                        ThriftValidation.asIFilterUsingPageToken(spStartC1EndC4, cfs.metadata, null),
//                        100,
//                        System.currentTimeMillis(),
//                        true,
//                        false),
//                5,
//                ImmutableList.of(pageTokenEnd, pageTokenEnd));
//    }
//
//    @Test
//    public void testRangeSlicePageTokenWithDuplicates()
//    {
//        String keyspaceName = KEYSPACE1;
//        String cfName = CF_STANDARD1;
//        Keyspace keyspace = Keyspace.open(keyspaceName);
//        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);
//        cfs.clearUnsafe();
//
//        Cell[] cols = new Cell[5];
//        Cell[] colsLaterTs = new Cell[5];
//        for (int i = 0; i < 5; i++)
//        {
//            cols[i] = column("c" + i, "value", 1);
//            colsLaterTs[i] = column("c" + i, "value", 2);
//        }
//        putColsStandard(cfs, Util.dk("A"), cols[0], cols[1], cols[2], cols[3], cols[4]);
//        putColsStandard(cfs, Util.dk("B"), cols[0], cols[0], cols[0], cols[0], cols[1]);
//        putColsStandard(cfs, Util.dk("C"), cols[0], cols[1], cols[2], cols[3], cols[4]);
//        putColsStandard(cfs, Util.dk("D"), cols[0], cols[1], cols[2], cols[3], cols[4]);
//        cfs.forceBlockingFlush();
//        putColsStandard(cfs, Util.dk("C"), colsLaterTs[0], colsLaterTs[1], colsLaterTs[4]);
//        putColsStandard(cfs, Util.dk("D"), colsLaterTs[3], colsLaterTs[4]);
//        cfs.forceBlockingFlush();
//
//        SlicePredicate spAll = new SlicePredicate();
//        spAll.setSlice_range(new SliceRange());
//        spAll.getSlice_range().setCount(1);
//        spAll.getSlice_range().setStart(ArrayUtils.EMPTY_BYTE_ARRAY);
//        spAll.getSlice_range().setFinish(ArrayUtils.EMPTY_BYTE_ARRAY);
//
//        PageToken pageToken4 = PageToken.createPageToken(cols[4]);
//        PageToken pageTokenLater4 = PageToken.createPageToken(colsLaterTs[4]);
//        PageToken pageTokenEnd = PageToken.createPageTokenReachedEnd();
//
//        // rows: all ranges, columns: all ranges
//        assertTotalColCountAndPageTokens(cfs.getRangeSlice(Util.range("", ""),
//                        null,
//                        ThriftValidation.asIFilterUsingPageToken(spAll, cfs.metadata, null),
//                        100,
//                        System.currentTimeMillis(),
//                        true,
//                        false),
//                14,
//                ImmutableList.of(pageToken4, pageTokenEnd, pageTokenLater4, pageTokenLater4));
//        // rows: (B, D], columns: all ranges
//        assertTotalColCountAndPageTokens(cfs.getRangeSlice(Util.range("B", "D"),
//                        null,
//                        ThriftValidation.asIFilterUsingPageToken(spAll, cfs.metadata, null),
//                        100,
//                        System.currentTimeMillis(),
//                        true,
//                        false),
//                8,
//                ImmutableList.of(pageTokenLater4, pageTokenLater4));
//    }
//
//    @Test
//    public void testRangeSlicePageTokenWithRangeTombstones()
//    {
//        String keyspaceName = KEYSPACE1;
//        String cfName = CF_STANDARD1;
//        Keyspace keyspace = Keyspace.open(keyspaceName);
//        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);
//        cfs.clearUnsafe();
//
//        Cell[] cols = new Cell[7];
//        for (int i = 0; i < 7; i++)
//        {
//            cols[i] = column("c" + i, "value", 1);
//        }
//        putColsStandard(cfs, Util.dk("A"), cols[0], cols[1], cols[2], cols[3], cols[4], cols[5], cols[6]);
//        putColsStandard(cfs, Util.dk("B"), cols[0], cols[1], cols[2], cols[3], cols[4], cols[5]);
//        putColsStandard(cfs, Util.dk("C"), cols[0], cols[1], cols[2], cols[3], cols[4]);
//        putColsStandard(cfs, Util.dk("D"), cols[0], cols[1], cols[2], cols[3]);
//        putColsStandard(cfs, Util.dk("E"), cols[0], cols[1], cols[2]);
//        cfs.forceBlockingFlush();
//        deleteRange(cfs, Util.dk("A"), tombstone("c0", "c1", 2, 2));
//        deleteRange(cfs, Util.dk("B"), tombstone("c0", "c1", 2, 2));
//        deleteRange(cfs, Util.dk("D"), tombstone("c0", "c3", 2, 2));
//
//        SlicePredicate spAll = new SlicePredicate();
//        spAll.setSlice_range(new SliceRange());
//        spAll.getSlice_range().setCount(1);
//        spAll.getSlice_range().setStart(ArrayUtils.EMPTY_BYTE_ARRAY);
//        spAll.getSlice_range().setFinish(ArrayUtils.EMPTY_BYTE_ARRAY);
//
//        SlicePredicate spStartC1 = new SlicePredicate();
//        spStartC1.setSlice_range(new SliceRange());
//        spStartC1.getSlice_range().setCount(1);
//        spStartC1.getSlice_range().setStart(ByteBufferUtil.bytes("c1"));
//        spStartC1.getSlice_range().setFinish(ArrayUtils.EMPTY_BYTE_ARRAY);
//
//        SlicePredicate spEndC4 = new SlicePredicate();
//        spEndC4.setSlice_range(new SliceRange());
//        spEndC4.getSlice_range().setCount(1);
//        spEndC4.getSlice_range().setStart(ArrayUtils.EMPTY_BYTE_ARRAY);
//        spEndC4.getSlice_range().setFinish(ByteBufferUtil.bytes("c4"));
//
//        SlicePredicate spStartC1EndC4 = new SlicePredicate();
//        spStartC1EndC4.setSlice_range(new SliceRange());
//        spStartC1EndC4.getSlice_range().setCount(1);
//        spStartC1EndC4.getSlice_range().setStart(ByteBufferUtil.bytes("c1"));
//        spStartC1EndC4.getSlice_range().setFinish(ByteBufferUtil.bytes("c4"));
//
//        PageToken pageToken4 = PageToken.createPageToken(cols[4]);
//        PageToken pageToken5 = PageToken.createPageToken(cols[5]);
//        PageToken pageTokenEnd = PageToken.createPageTokenReachedEnd();
//
//        // rows: all ranges, columns: all ranges
//        assertTotalColCountAndPageTokens(cfs.getRangeSlice(Util.range("", ""),
//                        null,
//                        ThriftValidation.asIFilterUsingPageToken(spAll, cfs.metadata, null),
//                        100,
//                        System.currentTimeMillis(),
//                        true,
//                        false),
//                11,
//                ImmutableList.of(pageToken4, pageToken4, pageToken4, pageTokenEnd, pageTokenEnd));
//        // rows: (C, E], columns: all ranges
//        assertTotalColCountAndPageTokens(cfs.getRangeSlice(Util.range("C", "E"),
//                        null,
//                        ThriftValidation.asIFilterUsingPageToken(spAll, cfs.metadata, null),
//                        100,
//                        System.currentTimeMillis(),
//                        true,
//                        false),
//                3,
//                ImmutableList.of(pageTokenEnd, pageTokenEnd));
//        // rows: all ranges, columns: [c1, \inf)
//        assertTotalColCountAndPageTokens(cfs.getRangeSlice(Util.range("", ""),
//                        null,
//                        ThriftValidation.asIFilterUsingPageToken(spStartC1, cfs.metadata, null),
//                        100,
//                        System.currentTimeMillis(),
//                        true,
//                        false),
//                12,
//                ImmutableList.of(pageToken5, pageToken5, pageTokenEnd, pageTokenEnd, pageTokenEnd));
//        // rows: (C, E], columns: [c1, \inf)
//        assertTotalColCountAndPageTokens(cfs.getRangeSlice(Util.range("C", "E"),
//                        null,
//                        ThriftValidation.asIFilterUsingPageToken(spStartC1, cfs.metadata, null),
//                        100,
//                        System.currentTimeMillis(),
//                        true,
//                        false),
//                2,
//                ImmutableList.of(pageTokenEnd, pageTokenEnd));
//        // rows: all ranges, columns: (-\inf, c4]
//        assertTotalColCountAndPageTokens(cfs.getRangeSlice(Util.range("", ""),
//                        null,
//                        ThriftValidation.asIFilterUsingPageToken(spEndC4, cfs.metadata, null),
//                        100,
//                        System.currentTimeMillis(),
//                        true,
//                        false),
//                11,
//                ImmutableList.of(pageToken4, pageToken4, pageToken4, pageTokenEnd, pageTokenEnd));
//        // rows: (C, E], columns: (-\inf, c4]
//        assertTotalColCountAndPageTokens(cfs.getRangeSlice(Util.range("C", "E"),
//                        null,
//                        ThriftValidation.asIFilterUsingPageToken(spEndC4, cfs.metadata, null),
//                        100,
//                        System.currentTimeMillis(),
//                        true,
//                        false),
//                3,
//                ImmutableList.of(pageTokenEnd, pageTokenEnd));
//        // rows: all ranges, columns: [c1, c4]
//        assertTotalColCountAndPageTokens(cfs.getRangeSlice(Util.range("", ""),
//                        null,
//                        ThriftValidation.asIFilterUsingPageToken(spStartC1EndC4, cfs.metadata, null),
//                        100,
//                        System.currentTimeMillis(),
//                        true,
//                        false),
//                12,
//                ImmutableList.of(pageTokenEnd, pageTokenEnd, pageTokenEnd, pageTokenEnd, pageTokenEnd));
//        // rows: (C, E], columns: [c1, c4]
//        assertTotalColCountAndPageTokens(cfs.getRangeSlice(Util.range("C", "E"),
//                        null,
//                        ThriftValidation.asIFilterUsingPageToken(spStartC1EndC4, cfs.metadata, null),
//                        100,
//                        System.currentTimeMillis(),
//                        true,
//                        false),
//                2,
//                ImmutableList.of(pageTokenEnd, pageTokenEnd));
//    }
//
//    @Test
//    public void testRangeSlicesPageTokenWithPointTombstones()
//    {
//        String keyspaceName = KEYSPACE1;
//        String cfName = CF_STANDARD1;
//        Keyspace keyspace = Keyspace.open(keyspaceName);
//        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);
//        cfs.clearUnsafe();
//
//        Cell[] cols = new Cell[7];
//        Cell[] pointTombstones = new Cell[7];
//        for (int i = 0; i < 7; i++)
//        {
//            cols[i] = column("c" + i, "value", 1);
//            pointTombstones[i] = expiredColumn("c" + i, "value", 1);
//        }
//        Cell pointTombstoneBetweenC1C2 = expiredColumn("c11", "value", 1);
//        Cell pointTombstoneBetweenC3C4 = expiredColumn("c31", "value", 1);
//        putColsStandard(cfs, Util.dk("A"), cols[0], cols[1], cols[2], cols[3], cols[4], cols[5], cols[6]);
//        putColsStandard(cfs, Util.dk("B"), cols[0], cols[1], cols[2], cols[3], cols[4], cols[5]);
//        putColsStandard(cfs, Util.dk("C"), cols[0], cols[1], cols[2], cols[3], cols[4]);
//        putColsStandard(cfs, Util.dk("D"), cols[0], cols[1], cols[2], cols[3]);
//        putColsStandard(cfs, Util.dk("E"), cols[0], cols[1], cols[2]);
//        cfs.forceBlockingFlush();
//        putColsStandard(cfs, Util.dk("A"), pointTombstones[0], pointTombstones[1], pointTombstoneBetweenC3C4);
//        putColsStandard(cfs, Util.dk("B"), pointTombstoneBetweenC1C2);
//        putColsStandard(cfs, Util.dk("C"), pointTombstones[0], pointTombstones[1]);
//        putColsStandard(cfs, Util.dk("D"), pointTombstones[0], pointTombstones[1], pointTombstones[2], pointTombstones[3]);
//        cfs.forceBlockingFlush();
//
//        SlicePredicate spAll = new SlicePredicate();
//        spAll.setSlice_range(new SliceRange());
//        spAll.getSlice_range().setCount(1);
//        spAll.getSlice_range().setStart(ArrayUtils.EMPTY_BYTE_ARRAY);
//        spAll.getSlice_range().setFinish(ArrayUtils.EMPTY_BYTE_ARRAY);
//
//        SlicePredicate spStartC1 = new SlicePredicate();
//        spStartC1.setSlice_range(new SliceRange());
//        spStartC1.getSlice_range().setCount(1);
//        spStartC1.getSlice_range().setStart(ByteBufferUtil.bytes("c1"));
//        spStartC1.getSlice_range().setFinish(ArrayUtils.EMPTY_BYTE_ARRAY);
//
//        SlicePredicate spEndC4 = new SlicePredicate();
//        spEndC4.setSlice_range(new SliceRange());
//        spEndC4.getSlice_range().setCount(1);
//        spEndC4.getSlice_range().setStart(ArrayUtils.EMPTY_BYTE_ARRAY);
//        spEndC4.getSlice_range().setFinish(ByteBufferUtil.bytes("c4"));
//
//        SlicePredicate spStartC1EndC4 = new SlicePredicate();
//        spStartC1EndC4.setSlice_range(new SliceRange());
//        spStartC1EndC4.getSlice_range().setCount(1);
//        spStartC1EndC4.getSlice_range().setStart(ByteBufferUtil.bytes("c1"));
//        spStartC1EndC4.getSlice_range().setFinish(ByteBufferUtil.bytes("c4"));
//
//        PageToken pageTokenBetweenC3C4 = PageToken.createPageToken(pointTombstoneBetweenC3C4);
//        PageToken pageToken3 = PageToken.createPageToken(cols[3]);
//        PageToken pageToken4 = PageToken.createPageToken(cols[4]);
//        PageToken pageTokenEnd = PageToken.createPageTokenReachedEnd();
//
//        // rows: all ranges, columns: all ranges
//        assertTotalColCountAndPageTokens(cfs.getRangeSlice(Util.range("", ""),
//                        null,
//                        ThriftValidation.asIFilterUsingPageToken(spAll, cfs.metadata, null),
//                        100,
//                        System.currentTimeMillis(),
//                        true,
//                        false),
//                10,
//                ImmutableList.of(pageTokenBetweenC3C4, pageToken3, pageToken4, pageTokenEnd, pageTokenEnd));
//        // rows: (C, E], columns: all ranges
//        assertTotalColCountAndPageTokens(cfs.getRangeSlice(Util.range("C", "E"),
//                        null,
//                        ThriftValidation.asIFilterUsingPageToken(spAll, cfs.metadata, null),
//                        100,
//                        System.currentTimeMillis(),
//                        true,
//                        false),
//                3,
//                ImmutableList.of(pageTokenEnd, pageTokenEnd));
//        // rows: all ranges, columns: [c1, \inf)
//        assertTotalColCountAndPageTokens(cfs.getRangeSlice(Util.range("", ""),
//                        null,
//                        ThriftValidation.asIFilterUsingPageToken(spStartC1, cfs.metadata, null),
//                        100,
//                        System.currentTimeMillis(),
//                        true,
//                        false),
//                10,
//                ImmutableList.of(pageToken4, pageToken4, pageTokenEnd, pageTokenEnd, pageTokenEnd));
//        // rows: (C, E], columns: [c1, \inf)
//        assertTotalColCountAndPageTokens(cfs.getRangeSlice(Util.range("C", "E"),
//                        null,
//                        ThriftValidation.asIFilterUsingPageToken(spStartC1, cfs.metadata, null),
//                        100,
//                        System.currentTimeMillis(),
//                        true,
//                        false),
//                2,
//                ImmutableList.of(pageTokenEnd, pageTokenEnd));
//        // rows: all ranges, columns: (-\inf, c4]
//        assertTotalColCountAndPageTokens(cfs.getRangeSlice(Util.range("", ""),
//                        null,
//                        ThriftValidation.asIFilterUsingPageToken(spEndC4, cfs.metadata, null),
//                        100,
//                        System.currentTimeMillis(),
//                        true,
//                        false),
//                10,
//                ImmutableList.of(pageTokenBetweenC3C4, pageToken3, pageToken4, pageTokenEnd, pageTokenEnd));
//        // rows: (C, E], columns: (-\inf, c4]
//        assertTotalColCountAndPageTokens(cfs.getRangeSlice(Util.range("C", "E"),
//                        null,
//                        ThriftValidation.asIFilterUsingPageToken(spEndC4, cfs.metadata, null),
//                        100,
//                        System.currentTimeMillis(),
//                        true,
//                        false),
//                3,
//                ImmutableList.of(pageTokenEnd, pageTokenEnd));
//        // rows: all ranges, columns: [c1, c4]
//        assertTotalColCountAndPageTokens(cfs.getRangeSlice(Util.range("", ""),
//                        null,
//                        ThriftValidation.asIFilterUsingPageToken(spStartC1EndC4, cfs.metadata, null),
//                        100,
//                        System.currentTimeMillis(),
//                        true,
//                        false),
//                10,
//                ImmutableList.of(pageToken4, pageToken4, pageTokenEnd, pageTokenEnd, pageTokenEnd));
//        // rows: (C, E], columns: [c1, c4]
//        assertTotalColCountAndPageTokens(cfs.getRangeSlice(Util.range("C", "E"),
//                        null,
//                        ThriftValidation.asIFilterUsingPageToken(spStartC1EndC4, cfs.metadata, null),
//                        100,
//                        System.currentTimeMillis(),
//                        true,
//                        false),
//                2,
//                ImmutableList.of(pageTokenEnd, pageTokenEnd));
//    }
//
//    @Test
//    public void testGetRowSliceByRangeUsingPageToken()
//    {
//        DecoratedKey key = TEST_SLICE_KEY;
//        Keyspace keyspace = Keyspace.open(KEYSPACE1);
//        ColumnFamilyStore cfStore = keyspace.getColumnFamilyStore("Standard1");
//        ColumnFamily cf = ArrayBackedSortedColumns.factory.create(KEYSPACE1, "Standard1");
//        // First write "a", "b", "c", "d", "e"
//        cf.addColumn(column("a", "val1", 1L));
//        cf.addColumn(column("b", "val2", 1L));
//        cf.addColumn(column("c", "val3", 1L));
//        cf.addColumn(column("d", "val4", 1L));
//        cf.addColumn(column("e", "val5", 1L));
//        Mutation rm = new Mutation(KEYSPACE1, key.getKey(), cf);
//        rm.applyUnsafe();
//
//        PageToken pageTokenE = PageToken.createPageToken(column("e", "val5", 1L));
//        PageToken pageTokenEnd = PageToken.createPageTokenReachedEnd();
//
//        cf = cfStore.getColumnFamilyUsingPageToken(key, cellname("a"), cellname("e"), false, 100, System.currentTimeMillis());
//        assertEquals(4, cf.getColumnCount());
//        assertEquals(pageTokenE, cf.pageToken());
//
//        cf = cfStore.getColumnFamilyUsingPageToken(key, cellname("b"), cellname("d"), false, 100, System.currentTimeMillis());
//        assertEquals(3, cf.getColumnCount());
//        assertEquals(pageTokenEnd, cf.pageToken());
//
//        cf = cfStore.getColumnFamilyUsingPageToken(key, cellname("b"), cellname("e"), false, 100, System.currentTimeMillis());
//        assertEquals(4, cf.getColumnCount());
//        assertEquals(pageTokenEnd, cf.pageToken());
//
//        cf = cfStore.getColumnFamilyUsingPageToken(key, cellname("e"), cellname("g"), false, 100, System.currentTimeMillis());
//        assertEquals(1, cf.getColumnCount());
//        assertEquals(pageTokenEnd, cf.pageToken());
//    }

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
    }

    private static void assertCellsAndPageToken(ColumnFamily cf, Collection<Cell> expectedCells, PageToken expectedPageToken)
    {
        assertNotNull(cf);
        assertTrue(CollectionUtils.isEqualCollection(cf.getSortedColumns(), expectedCells));
        assertEquals(cf.pageToken(), expectedPageToken);
    }
}
