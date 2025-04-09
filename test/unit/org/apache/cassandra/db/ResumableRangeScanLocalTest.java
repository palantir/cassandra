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

import com.google.common.collect.ImmutableList;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.filter.PageToken;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LexicalUUIDType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.ThriftValidation;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Random;

import static org.apache.cassandra.Util.*;
import static org.apache.cassandra.Util.expiredColumn;
import static org.junit.Assert.assertEquals;

public class ResumableRangeScanLocalTest
{
    static byte[] bytes1, bytes2;
    public static final String KEYSPACE1 = "ColumnFamilyStoreTest1";
    public static final String KEYSPACE2 = "ColumnFamilyStoreTest2";
    public static final String KEYSPACE3 = "ColumnFamilyStoreTest3";
    public static final String KEYSPACE4 = "PerRowSecondaryIndex";
    public static final String CF_STANDARD1 = "Standard1";
    public static final String CF_STANDARD2 = "Standard2";
    public static final String CF_STANDARD3 = "Standard3";
    public static final String CF_STANDARD4 = "Standard4";
    public static final String CF_STANDARD5 = "Standard5";
    public static final String CF_STANDARD6 = "Standard6";
    public static final String CF_STANDARD7 = "Standard7";
    public static final String CF_STANDARD8 = "Standard8";
    public static final String CF_STANDARD9 = "Standard9";
    public static final String CF_STANDARDINT = "StandardInteger1";
    public static final String CF_SUPER1 = "Super1";
    public static final String CF_SUPER6 = "Super6";
    public static final String CF_INDEX1 = "Indexed1";
    public static final String CF_INDEX2 = "Indexed2";
    public static final String CF_INDEX3 = "Indexed3";

    static
    {
        Random random = new Random();
        bytes1 = new byte[1024];
        bytes2 = new byte[128];
        random.nextBytes(bytes1);
        random.nextBytes(bytes2);
    }

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                SimpleStrategy.class,
                KSMetaData.optsWithRF(1),
                SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1),
                SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD2),
                SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD3),
                SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD4),
                SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD5),
                SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD6),
                SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD7),
                SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD8),
                SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD9),
                SchemaLoader.indexCFMD(KEYSPACE1, CF_INDEX1, true),
                SchemaLoader.indexCFMD(KEYSPACE1, CF_INDEX2, false),
                SchemaLoader.superCFMD(KEYSPACE1, CF_SUPER1, LongType.instance),
                SchemaLoader.superCFMD(KEYSPACE1, CF_SUPER6, LexicalUUIDType.instance, UTF8Type.instance),
                SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARDINT, IntegerType.instance));
        SchemaLoader.createKeyspace(KEYSPACE2,
                SimpleStrategy.class,
                KSMetaData.optsWithRF(1),
                SchemaLoader.standardCFMD(KEYSPACE2, CF_STANDARD1),
                SchemaLoader.indexCFMD(KEYSPACE2, CF_INDEX1, true),
                SchemaLoader.compositeIndexCFMD(KEYSPACE2, CF_INDEX2, true),
                SchemaLoader.compositeIndexCFMD(KEYSPACE2, CF_INDEX3, true).gcGraceSeconds(0));
        SchemaLoader.createKeyspace(KEYSPACE3,
                SimpleStrategy.class,
                KSMetaData.optsWithRF(5),
                SchemaLoader.indexCFMD(KEYSPACE3, CF_INDEX1, true));
        SchemaLoader.createKeyspace(KEYSPACE4,
                SimpleStrategy.class,
                KSMetaData.optsWithRF(1),
                SchemaLoader.perRowIndexedCFMD(KEYSPACE4, "Indexed1"));
    }

    @Test
    public void testRangeSlicePageTokenVariousSlices()
    {
        String keyspaceName = KEYSPACE1;
        String cfName = CF_STANDARD1;
        Keyspace keyspace = Keyspace.open(keyspaceName);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);
        cfs.clearUnsafe();

        Cell[] cols = new Cell[7];
        for (int i = 0; i < 7; i++)
        {
            cols[i] = column("c" + i, "value", 1);
        }
        putColsStandard(cfs, Util.dk("A"), cols[0], cols[1], cols[2], cols[3], cols[4], cols[5], cols[6]);
        putColsStandard(cfs, Util.dk("B"), cols[0], cols[1], cols[2], cols[3], cols[4], cols[5]);
        putColsStandard(cfs, Util.dk("C"), cols[0], cols[1], cols[2], cols[3], cols[4]);
        putColsStandard(cfs, Util.dk("D"), cols[0], cols[1], cols[2], cols[3]);
        putColsStandard(cfs, Util.dk("E"), cols[0], cols[1], cols[2]);
        cfs.forceBlockingFlush();

        SlicePredicate spAll = new SlicePredicate();
        spAll.setSlice_range(new SliceRange());
        spAll.getSlice_range().setCount(1);
        spAll.getSlice_range().setStart(ArrayUtils.EMPTY_BYTE_ARRAY);
        spAll.getSlice_range().setFinish(ArrayUtils.EMPTY_BYTE_ARRAY);

        SlicePredicate spStartC1 = new SlicePredicate();
        spStartC1.setSlice_range(new SliceRange());
        spStartC1.getSlice_range().setCount(1);
        spStartC1.getSlice_range().setStart(ByteBufferUtil.bytes("c1"));
        spStartC1.getSlice_range().setFinish(ArrayUtils.EMPTY_BYTE_ARRAY);

        SlicePredicate spEndC4 = new SlicePredicate();
        spEndC4.setSlice_range(new SliceRange());
        spEndC4.getSlice_range().setCount(1);
        spEndC4.getSlice_range().setStart(ArrayUtils.EMPTY_BYTE_ARRAY);
        spEndC4.getSlice_range().setFinish(ByteBufferUtil.bytes("c4"));

        SlicePredicate spStartC1EndC4 = new SlicePredicate();
        spStartC1EndC4.setSlice_range(new SliceRange());
        spStartC1EndC4.getSlice_range().setCount(1);
        spStartC1EndC4.getSlice_range().setStart(ByteBufferUtil.bytes("c1"));
        spStartC1EndC4.getSlice_range().setFinish(ByteBufferUtil.bytes("c4"));

        PageToken pageToken4 = PageToken.createPageToken(cols[4]);
        PageToken pageToken5 = PageToken.createPageToken(cols[5]);
        PageToken pageTokenEnd = PageToken.createPageTokenReachedEnd();

        // rows: all ranges, columns: all ranges
        assertTotalColCountAndPageTokens(cfs.getRangeSlice(Util.range("", ""),
                        null,
                        ThriftValidation.asIFilterUsingPageToken(spAll, cfs.metadata, null),
                        100,
                        System.currentTimeMillis(),
                        true,
                        false),
                19,
                ImmutableList.of(pageToken4, pageToken4, pageToken4, pageTokenEnd, pageTokenEnd));
        // rows: (C, E], columns: all ranges
        assertTotalColCountAndPageTokens(cfs.getRangeSlice(Util.range("C", "E"),
                        null,
                        ThriftValidation.asIFilterUsingPageToken(spAll, cfs.metadata, null),
                        100,
                        System.currentTimeMillis(),
                        true,
                        false),
                7,
                ImmutableList.of(pageTokenEnd, pageTokenEnd));
        // rows: all ranges, columns: [c1, \inf)
        assertTotalColCountAndPageTokens(cfs.getRangeSlice(Util.range("", ""),
                        null,
                        ThriftValidation.asIFilterUsingPageToken(spStartC1, cfs.metadata, null),
                        100,
                        System.currentTimeMillis(),
                        true,
                        false),
                17,
                ImmutableList.of(pageToken5, pageToken5, pageTokenEnd, pageTokenEnd, pageTokenEnd));
        // rows: (C, E], columns: [c1, \inf)
        assertTotalColCountAndPageTokens(cfs.getRangeSlice(Util.range("C", "E"),
                        null,
                        ThriftValidation.asIFilterUsingPageToken(spStartC1, cfs.metadata, null),
                        100,
                        System.currentTimeMillis(),
                        true,
                        false),
                5,
                ImmutableList.of(pageTokenEnd, pageTokenEnd));
        // rows: all ranges, columns: (-\inf, c4]
        assertTotalColCountAndPageTokens(cfs.getRangeSlice(Util.range("", ""),
                        null,
                        ThriftValidation.asIFilterUsingPageToken(spEndC4, cfs.metadata, null),
                        100,
                        System.currentTimeMillis(),
                        true,
                        false),
                19,
                ImmutableList.of(pageToken4, pageToken4, pageToken4, pageTokenEnd, pageTokenEnd));
        // rows: (C, E], columns: (-\inf, c4]
        assertTotalColCountAndPageTokens(cfs.getRangeSlice(Util.range("C", "E"),
                        null,
                        ThriftValidation.asIFilterUsingPageToken(spEndC4, cfs.metadata, null),
                        100,
                        System.currentTimeMillis(),
                        true,
                        false),
                7,
                ImmutableList.of(pageTokenEnd, pageTokenEnd));
        // rows: all ranges, columns: [c1, c4]
        assertTotalColCountAndPageTokens(cfs.getRangeSlice(Util.range("", ""),
                        null,
                        ThriftValidation.asIFilterUsingPageToken(spStartC1EndC4, cfs.metadata, null),
                        100,
                        System.currentTimeMillis(),
                        true,
                        false),
                17,
                ImmutableList.of(pageTokenEnd, pageTokenEnd, pageTokenEnd, pageTokenEnd, pageTokenEnd));
        // rows: (C, E], columns: [c1, c4]
        assertTotalColCountAndPageTokens(cfs.getRangeSlice(Util.range("C", "E"),
                        null,
                        ThriftValidation.asIFilterUsingPageToken(spStartC1EndC4, cfs.metadata, null),
                        100,
                        System.currentTimeMillis(),
                        true,
                        false),
                5,
                ImmutableList.of(pageTokenEnd, pageTokenEnd));
    }

    @Test
    public void testRangeSlicePageTokenWithDuplicates()
    {
        String keyspaceName = KEYSPACE1;
        String cfName = CF_STANDARD1;
        Keyspace keyspace = Keyspace.open(keyspaceName);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);
        cfs.clearUnsafe();

        Cell[] cols = new Cell[5];
        Cell[] colsLaterTs = new Cell[5];
        for (int i = 0; i < 5; i++)
        {
            cols[i] = column("c" + i, "value", 1);
            colsLaterTs[i] = column("c" + i, "value", 2);
        }
        putColsStandard(cfs, Util.dk("A"), cols[0], cols[1], cols[2], cols[3], cols[4]);
        putColsStandard(cfs, Util.dk("B"), cols[0], cols[0], cols[0], cols[0], cols[1]);
        putColsStandard(cfs, Util.dk("C"), cols[0], cols[1], cols[2], cols[3], cols[4]);
        putColsStandard(cfs, Util.dk("D"), cols[0], cols[1], cols[2], cols[3], cols[4]);
        cfs.forceBlockingFlush();
        putColsStandard(cfs, Util.dk("C"), colsLaterTs[0], colsLaterTs[1], colsLaterTs[4]);
        putColsStandard(cfs, Util.dk("D"), colsLaterTs[3], colsLaterTs[4]);
        cfs.forceBlockingFlush();

        SlicePredicate spAll = new SlicePredicate();
        spAll.setSlice_range(new SliceRange());
        spAll.getSlice_range().setCount(1);
        spAll.getSlice_range().setStart(ArrayUtils.EMPTY_BYTE_ARRAY);
        spAll.getSlice_range().setFinish(ArrayUtils.EMPTY_BYTE_ARRAY);

        PageToken pageToken4 = PageToken.createPageToken(cols[4]);
        PageToken pageTokenLater4 = PageToken.createPageToken(colsLaterTs[4]);
        PageToken pageTokenEnd = PageToken.createPageTokenReachedEnd();

        // rows: all ranges, columns: all ranges
        assertTotalColCountAndPageTokens(cfs.getRangeSlice(Util.range("", ""),
                        null,
                        ThriftValidation.asIFilterUsingPageToken(spAll, cfs.metadata, null),
                        100,
                        System.currentTimeMillis(),
                        true,
                        false),
                14,
                ImmutableList.of(pageToken4, pageTokenEnd, pageTokenLater4, pageTokenLater4));
        // rows: (B, D], columns: all ranges
        assertTotalColCountAndPageTokens(cfs.getRangeSlice(Util.range("B", "D"),
                        null,
                        ThriftValidation.asIFilterUsingPageToken(spAll, cfs.metadata, null),
                        100,
                        System.currentTimeMillis(),
                        true,
                        false),
                8,
                ImmutableList.of(pageTokenLater4, pageTokenLater4));
    }

    @Test
    public void testRangeSlicePageTokenWithRangeTombstones()
    {
        String keyspaceName = KEYSPACE1;
        String cfName = CF_STANDARD1;
        Keyspace keyspace = Keyspace.open(keyspaceName);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);
        cfs.clearUnsafe();

        Cell[] cols = new Cell[7];
        for (int i = 0; i < 7; i++)
        {
            cols[i] = column("c" + i, "value", 1);
        }
        putColsStandard(cfs, Util.dk("A"), cols[0], cols[1], cols[2], cols[3], cols[4], cols[5], cols[6]);
        putColsStandard(cfs, Util.dk("B"), cols[0], cols[1], cols[2], cols[3], cols[4], cols[5]);
        putColsStandard(cfs, Util.dk("C"), cols[0], cols[1], cols[2], cols[3], cols[4]);
        putColsStandard(cfs, Util.dk("D"), cols[0], cols[1], cols[2], cols[3]);
        putColsStandard(cfs, Util.dk("E"), cols[0], cols[1], cols[2]);
        cfs.forceBlockingFlush();
        deleteRange(cfs, Util.dk("A"), tombstone("c0", "c1", 2, 2));
        deleteRange(cfs, Util.dk("B"), tombstone("c0", "c1", 2, 2));
        deleteRange(cfs, Util.dk("D"), tombstone("c0", "c3", 2, 2));

        SlicePredicate spAll = new SlicePredicate();
        spAll.setSlice_range(new SliceRange());
        spAll.getSlice_range().setCount(1);
        spAll.getSlice_range().setStart(ArrayUtils.EMPTY_BYTE_ARRAY);
        spAll.getSlice_range().setFinish(ArrayUtils.EMPTY_BYTE_ARRAY);

        SlicePredicate spStartC1 = new SlicePredicate();
        spStartC1.setSlice_range(new SliceRange());
        spStartC1.getSlice_range().setCount(1);
        spStartC1.getSlice_range().setStart(ByteBufferUtil.bytes("c1"));
        spStartC1.getSlice_range().setFinish(ArrayUtils.EMPTY_BYTE_ARRAY);

        SlicePredicate spEndC4 = new SlicePredicate();
        spEndC4.setSlice_range(new SliceRange());
        spEndC4.getSlice_range().setCount(1);
        spEndC4.getSlice_range().setStart(ArrayUtils.EMPTY_BYTE_ARRAY);
        spEndC4.getSlice_range().setFinish(ByteBufferUtil.bytes("c4"));

        SlicePredicate spStartC1EndC4 = new SlicePredicate();
        spStartC1EndC4.setSlice_range(new SliceRange());
        spStartC1EndC4.getSlice_range().setCount(1);
        spStartC1EndC4.getSlice_range().setStart(ByteBufferUtil.bytes("c1"));
        spStartC1EndC4.getSlice_range().setFinish(ByteBufferUtil.bytes("c4"));

        PageToken pageToken4 = PageToken.createPageToken(cols[4]);
        PageToken pageToken5 = PageToken.createPageToken(cols[5]);
        PageToken pageTokenEnd = PageToken.createPageTokenReachedEnd();

        // rows: all ranges, columns: all ranges
        assertTotalColCountAndPageTokens(cfs.getRangeSlice(Util.range("", ""),
                        null,
                        ThriftValidation.asIFilterUsingPageToken(spAll, cfs.metadata, null),
                        100,
                        System.currentTimeMillis(),
                        true,
                        false),
                11,
                ImmutableList.of(pageToken4, pageToken4, pageToken4, pageTokenEnd, pageTokenEnd));
        // rows: (C, E], columns: all ranges
        assertTotalColCountAndPageTokens(cfs.getRangeSlice(Util.range("C", "E"),
                        null,
                        ThriftValidation.asIFilterUsingPageToken(spAll, cfs.metadata, null),
                        100,
                        System.currentTimeMillis(),
                        true,
                        false),
                3,
                ImmutableList.of(pageTokenEnd, pageTokenEnd));
        // rows: all ranges, columns: [c1, \inf)
        assertTotalColCountAndPageTokens(cfs.getRangeSlice(Util.range("", ""),
                        null,
                        ThriftValidation.asIFilterUsingPageToken(spStartC1, cfs.metadata, null),
                        100,
                        System.currentTimeMillis(),
                        true,
                        false),
                12,
                ImmutableList.of(pageToken5, pageToken5, pageTokenEnd, pageTokenEnd, pageTokenEnd));
        // rows: (C, E], columns: [c1, \inf)
        assertTotalColCountAndPageTokens(cfs.getRangeSlice(Util.range("C", "E"),
                        null,
                        ThriftValidation.asIFilterUsingPageToken(spStartC1, cfs.metadata, null),
                        100,
                        System.currentTimeMillis(),
                        true,
                        false),
                2,
                ImmutableList.of(pageTokenEnd, pageTokenEnd));
        // rows: all ranges, columns: (-\inf, c4]
        assertTotalColCountAndPageTokens(cfs.getRangeSlice(Util.range("", ""),
                        null,
                        ThriftValidation.asIFilterUsingPageToken(spEndC4, cfs.metadata, null),
                        100,
                        System.currentTimeMillis(),
                        true,
                        false),
                11,
                ImmutableList.of(pageToken4, pageToken4, pageToken4, pageTokenEnd, pageTokenEnd));
        // rows: (C, E], columns: (-\inf, c4]
        assertTotalColCountAndPageTokens(cfs.getRangeSlice(Util.range("C", "E"),
                        null,
                        ThriftValidation.asIFilterUsingPageToken(spEndC4, cfs.metadata, null),
                        100,
                        System.currentTimeMillis(),
                        true,
                        false),
                3,
                ImmutableList.of(pageTokenEnd, pageTokenEnd));
        // rows: all ranges, columns: [c1, c4]
        assertTotalColCountAndPageTokens(cfs.getRangeSlice(Util.range("", ""),
                        null,
                        ThriftValidation.asIFilterUsingPageToken(spStartC1EndC4, cfs.metadata, null),
                        100,
                        System.currentTimeMillis(),
                        true,
                        false),
                12,
                ImmutableList.of(pageTokenEnd, pageTokenEnd, pageTokenEnd, pageTokenEnd, pageTokenEnd));
        // rows: (C, E], columns: [c1, c4]
        assertTotalColCountAndPageTokens(cfs.getRangeSlice(Util.range("C", "E"),
                        null,
                        ThriftValidation.asIFilterUsingPageToken(spStartC1EndC4, cfs.metadata, null),
                        100,
                        System.currentTimeMillis(),
                        true,
                        false),
                2,
                ImmutableList.of(pageTokenEnd, pageTokenEnd));
    }

    @Test
    public void testRangeSlicesPageTokenWithPointTombstones()
    {
        String keyspaceName = KEYSPACE1;
        String cfName = CF_STANDARD1;
        Keyspace keyspace = Keyspace.open(keyspaceName);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);
        cfs.clearUnsafe();

        Cell[] cols = new Cell[7];
        Cell[] pointTombstones = new Cell[7];
        for (int i = 0; i < 7; i++)
        {
            cols[i] = column("c" + i, "value", 1);
            pointTombstones[i] = expiredColumn("c" + i, "value", 1);
        }
        Cell pointTombstoneBetweenC1C2 = expiredColumn("c11", "value", 1);
        Cell pointTombstoneBetweenC3C4 = expiredColumn("c31", "value", 1);
        putColsStandard(cfs, Util.dk("A"), cols[0], cols[1], cols[2], cols[3], cols[4], cols[5], cols[6]);
        putColsStandard(cfs, Util.dk("B"), cols[0], cols[1], cols[2], cols[3], cols[4], cols[5]);
        putColsStandard(cfs, Util.dk("C"), cols[0], cols[1], cols[2], cols[3], cols[4]);
        putColsStandard(cfs, Util.dk("D"), cols[0], cols[1], cols[2], cols[3]);
        putColsStandard(cfs, Util.dk("E"), cols[0], cols[1], cols[2]);
        cfs.forceBlockingFlush();
        putColsStandard(cfs, Util.dk("A"), pointTombstones[0], pointTombstones[1], pointTombstoneBetweenC3C4);
        putColsStandard(cfs, Util.dk("B"), pointTombstoneBetweenC1C2);
        putColsStandard(cfs, Util.dk("C"), pointTombstones[0], pointTombstones[1]);
        putColsStandard(cfs, Util.dk("D"), pointTombstones[0], pointTombstones[1], pointTombstones[2], pointTombstones[3]);
        cfs.forceBlockingFlush();

        SlicePredicate spAll = new SlicePredicate();
        spAll.setSlice_range(new SliceRange());
        spAll.getSlice_range().setCount(1);
        spAll.getSlice_range().setStart(ArrayUtils.EMPTY_BYTE_ARRAY);
        spAll.getSlice_range().setFinish(ArrayUtils.EMPTY_BYTE_ARRAY);

        SlicePredicate spStartC1 = new SlicePredicate();
        spStartC1.setSlice_range(new SliceRange());
        spStartC1.getSlice_range().setCount(1);
        spStartC1.getSlice_range().setStart(ByteBufferUtil.bytes("c1"));
        spStartC1.getSlice_range().setFinish(ArrayUtils.EMPTY_BYTE_ARRAY);

        SlicePredicate spEndC4 = new SlicePredicate();
        spEndC4.setSlice_range(new SliceRange());
        spEndC4.getSlice_range().setCount(1);
        spEndC4.getSlice_range().setStart(ArrayUtils.EMPTY_BYTE_ARRAY);
        spEndC4.getSlice_range().setFinish(ByteBufferUtil.bytes("c4"));

        SlicePredicate spStartC1EndC4 = new SlicePredicate();
        spStartC1EndC4.setSlice_range(new SliceRange());
        spStartC1EndC4.getSlice_range().setCount(1);
        spStartC1EndC4.getSlice_range().setStart(ByteBufferUtil.bytes("c1"));
        spStartC1EndC4.getSlice_range().setFinish(ByteBufferUtil.bytes("c4"));

        PageToken pageTokenBetweenC3C4 = PageToken.createPageToken(pointTombstoneBetweenC3C4);
        PageToken pageToken3 = PageToken.createPageToken(cols[3]);
        PageToken pageToken4 = PageToken.createPageToken(cols[4]);
        PageToken pageTokenEnd = PageToken.createPageTokenReachedEnd();

        // rows: all ranges, columns: all ranges
        assertTotalColCountAndPageTokens(cfs.getRangeSlice(Util.range("", ""),
                        null,
                        ThriftValidation.asIFilterUsingPageToken(spAll, cfs.metadata, null),
                        100,
                        System.currentTimeMillis(),
                        true,
                        false),
                10,
                ImmutableList.of(pageTokenBetweenC3C4, pageToken3, pageToken4, pageTokenEnd, pageTokenEnd));
        // rows: (C, E], columns: all ranges
        assertTotalColCountAndPageTokens(cfs.getRangeSlice(Util.range("C", "E"),
                        null,
                        ThriftValidation.asIFilterUsingPageToken(spAll, cfs.metadata, null),
                        100,
                        System.currentTimeMillis(),
                        true,
                        false),
                3,
                ImmutableList.of(pageTokenEnd, pageTokenEnd));
        // rows: all ranges, columns: [c1, \inf)
        assertTotalColCountAndPageTokens(cfs.getRangeSlice(Util.range("", ""),
                        null,
                        ThriftValidation.asIFilterUsingPageToken(spStartC1, cfs.metadata, null),
                        100,
                        System.currentTimeMillis(),
                        true,
                        false),
                10,
                ImmutableList.of(pageToken4, pageToken4, pageTokenEnd, pageTokenEnd, pageTokenEnd));
        // rows: (C, E], columns: [c1, \inf)
        assertTotalColCountAndPageTokens(cfs.getRangeSlice(Util.range("C", "E"),
                        null,
                        ThriftValidation.asIFilterUsingPageToken(spStartC1, cfs.metadata, null),
                        100,
                        System.currentTimeMillis(),
                        true,
                        false),
                2,
                ImmutableList.of(pageTokenEnd, pageTokenEnd));
        // rows: all ranges, columns: (-\inf, c4]
        assertTotalColCountAndPageTokens(cfs.getRangeSlice(Util.range("", ""),
                        null,
                        ThriftValidation.asIFilterUsingPageToken(spEndC4, cfs.metadata, null),
                        100,
                        System.currentTimeMillis(),
                        true,
                        false),
                10,
                ImmutableList.of(pageTokenBetweenC3C4, pageToken3, pageToken4, pageTokenEnd, pageTokenEnd));
        // rows: (C, E], columns: (-\inf, c4]
        assertTotalColCountAndPageTokens(cfs.getRangeSlice(Util.range("C", "E"),
                        null,
                        ThriftValidation.asIFilterUsingPageToken(spEndC4, cfs.metadata, null),
                        100,
                        System.currentTimeMillis(),
                        true,
                        false),
                3,
                ImmutableList.of(pageTokenEnd, pageTokenEnd));
        // rows: all ranges, columns: [c1, c4]
        assertTotalColCountAndPageTokens(cfs.getRangeSlice(Util.range("", ""),
                        null,
                        ThriftValidation.asIFilterUsingPageToken(spStartC1EndC4, cfs.metadata, null),
                        100,
                        System.currentTimeMillis(),
                        true,
                        false),
                10,
                ImmutableList.of(pageToken4, pageToken4, pageTokenEnd, pageTokenEnd, pageTokenEnd));
        // rows: (C, E], columns: [c1, c4]
        assertTotalColCountAndPageTokens(cfs.getRangeSlice(Util.range("C", "E"),
                        null,
                        ThriftValidation.asIFilterUsingPageToken(spStartC1EndC4, cfs.metadata, null),
                        100,
                        System.currentTimeMillis(),
                        true,
                        false),
                2,
                ImmutableList.of(pageTokenEnd, pageTokenEnd));
    }

    @Test
    public void testGetRowSliceByRangeUsingPageToken()
    {
        DecoratedKey key = TEST_SLICE_KEY;
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfStore = keyspace.getColumnFamilyStore("Standard1");
        ColumnFamily cf = ArrayBackedSortedColumns.factory.create(KEYSPACE1, "Standard1");
        // First write "a", "b", "c", "d", "e"
        cf.addColumn(column("a", "val1", 1L));
        cf.addColumn(column("b", "val2", 1L));
        cf.addColumn(column("c", "val3", 1L));
        cf.addColumn(column("d", "val4", 1L));
        cf.addColumn(column("e", "val5", 1L));
        Mutation rm = new Mutation(KEYSPACE1, key.getKey(), cf);
        rm.applyUnsafe();

        PageToken pageTokenE = PageToken.createPageToken(column("e", "val5", 1L));
        PageToken pageTokenEnd = PageToken.createPageTokenReachedEnd();

        cf = cfStore.getColumnFamilyUsingPageToken(key, cellname("a"), cellname("e"), false, 100, System.currentTimeMillis());
        assertEquals(4, cf.getColumnCount());
        assertEquals(pageTokenE, cf.pageToken());

        cf = cfStore.getColumnFamilyUsingPageToken(key, cellname("b"), cellname("d"), false, 100, System.currentTimeMillis());
        assertEquals(3, cf.getColumnCount());
        assertEquals(pageTokenEnd, cf.pageToken());

        cf = cfStore.getColumnFamilyUsingPageToken(key, cellname("b"), cellname("e"), false, 100, System.currentTimeMillis());
        assertEquals(4, cf.getColumnCount());
        assertEquals(pageTokenEnd, cf.pageToken());

        cf = cfStore.getColumnFamilyUsingPageToken(key, cellname("e"), cellname("g"), false, 100, System.currentTimeMillis());
        assertEquals(1, cf.getColumnCount());
        assertEquals(pageTokenEnd, cf.pageToken());
    }
}
