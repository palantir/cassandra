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

import org.junit.Test;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ArrayBackedSortedColumns;
import org.apache.cassandra.db.BufferCell;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.composites.CType;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.SimpleDenseCellNameType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.assertj.core.api.Assertions.assertThat;

public class FilterExperimentTest
{
    private static final long LIVE_DATA_TS = 99;
    private static final long TOMBSTONES_TS = 100;
    private static final int WRITE_TIME = 123;
    private static final CType type = new SimpleDenseCellNameType(UTF8Type.instance);
    private static final CFMetaData metadata =
    CFMetaData.denseCFMetaData("fake", "fake", UTF8Type.instance);

    @Test
    public void testAreEqual_trueIfEqual() {
        ColumnFamily cf = cf(value('a'), rangeDelete('b', 'c'));
        assertThat(FilterExperiment.areEqual(cf, cf)).isTrue();
    }

    @Test
    public void testAreEqual_trueIfEqual_cellwise() {
        ColumnFamily left = cf(value('a'), value('d'));
        ColumnFamily right = cf(value('a'), value('b'), rangeDelete('b', 'c'), value('d'));
        assertThat(FilterExperiment.areEqual(left, right)).isTrue();
    }

    @Test
    public void testAreEqual_falseIfMoreCells() {
        ColumnFamily left = cf(value('a'), value('b'));
        ColumnFamily right = cf(value('a'));
        assertThat(FilterExperiment.areEqual(left, right)).isFalse();
    }

    @Test
    public void testAreEqual_falseIfMoreCells_fromRangeTombstones() {
        ColumnFamily left = cf(value('a'), value('b'), rangeDelete('b', 'c'));
        ColumnFamily right = cf(value('a'), value('b'));
        assertThat(FilterExperiment.areEqual(left, right)).isFalse();
    }

    private static ColumnFamily cf(OnDiskAtom... atoms) {
        ColumnFamily cf = newCF();
        for (OnDiskAtom atom : atoms) {
            cf.addAtom(atom);
        }
        return cf;
    }

    private static ColumnFamily newCF() {
        return ArrayBackedSortedColumns.factory.create(metadata);
    }

    private static Cell value(char key, long timestamp) {
        return value(key, key, timestamp);
    }

    private static Cell value(char key) {
        return value(key, LIVE_DATA_TS);
    }

    private static Cell value(char key, char value, long timestamp) {
        return new BufferCell(key(key), ByteBufferUtil.bytes(String.valueOf(value)), timestamp);
    }

    private static RangeTombstone rangeDelete(char from, char to) {
        return new RangeTombstone(key(from), key(to), TOMBSTONES_TS, WRITE_TIME);
    }

    private static CellName key(char character) {
        return (CellName) type.fromByteBuffer(ByteBufferUtil.bytes(String.valueOf(character)));
    }

}
