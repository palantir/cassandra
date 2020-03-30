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

package org.apache.cassandra.db.filter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ArrayBackedSortedColumns;
import org.apache.cassandra.db.BufferCell;
import org.apache.cassandra.db.BufferDeletedCell;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.composites.CType;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.SimpleDenseCellNameType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.assertj.core.api.Assertions.assertThat;

public class QueryFilterTest {
    private static final CType type = new SimpleDenseCellNameType(UTF8Type.instance);
    private static final CFMetaData metadata =
        CFMetaData.denseCFMetaData("fake", "fake", UTF8Type.instance);
    private static final long LIVE_DATA_TS = 99;
    private static final long TOMBSTONES_TS = 100;
    private static final int WRITE_TIME = 123;

    @Test
    public void testCollateOnDiskAtom_handlesSuperLameDeoptimization_atStart() {
        List<Cell> left = ImmutableList.of(value('a'), value('b'), value('c'));
        List<OnDiskAtom> right = ImmutableList.of(nonDroppableRangeDelete('d', 'e'));
        ColumnFamily cf = newCF();
        collate(cf, 1, left.iterator(), right.iterator());
        assertThat(cf.deletionInfo().rangeIterator()).containsExactly(nonDroppableRangeDelete('d', 'e'));
        assertThat(cf.iterator()).containsExactly(value('a'));
    }

    @Test
    public void testCollateOnDiskAtom_handlesSuperLameDeoptimization_atEnd() {
        List<Cell> left = ImmutableList.of(value('a'));
        List<OnDiskAtom> right = ImmutableList.of(value('b'), nonDroppableRangeDelete('d', 'e'));
        ColumnFamily cf = newCF();
        collate(cf, 2, left.iterator(), right.iterator());
        assertThat(cf.deletionInfo().rangeIterator()).containsExactly(nonDroppableRangeDelete('d', 'e'));
        assertThat(cf.iterator()).containsExactly(value('a'), value('b'));
    }

    @Test
    public void testCollateOnDiskAtom_safeInPresenceOfRepeatedTombstones() {
        List<Cell> left = ImmutableList.of(value('a'), value('d'), value('e'), value('f'));
        List<OnDiskAtom> right = ImmutableList.of(rangeDelete('a', 'e'), value('a'), value('b'), rangeDelete('a', 'e'), value('g'));
        ColumnFamily cf = newCF();
        collate(cf, left.iterator(), right.iterator());
        assertThat(cf.deletionInfo().isLive()).isTrue();
        assertThat(cf.iterator()).containsExactly(value('f'), value('g'));
    }

    @Test
    public void testCollateOnDiskAtom_dropsUnnecessaryCellsAndTombstones() {
        List<Cell> left = ImmutableList.of(value('a'), value('d'));
        List<RangeTombstone> right = ImmutableList.of(rangeDelete('a', 'c'));
        ColumnFamily cf = newCF();
        collate(cf, left.iterator(), right.iterator());
        assertThat(cf.deletionInfo().isLive()).isTrue();
        assertThat(cf.iterator()).containsExactly(value('d'));
    }

    @Test
    public void testCollateOnDiskAtom_gathersNecessaryTombstones() {
        List<Cell> left = ImmutableList.of(value('a'), value('d'));
        List<RangeTombstone> right = ImmutableList.of(rangeDelete('a', 'c'), rangeDelete('b', 'c', TOMBSTONES_TS + 1));
        ColumnFamily cf = newCF();
        collate(cf, left.iterator(), right.iterator());
        assertThat(cf.deletionInfo().rangeIterator()).containsExactly(rangeDelete('a', 'c'));
        assertThat(cf.iterator()).containsExactly(value('d'));
    }

    @Test
    public void testCollateOnDiskAtom_merges() {
        List<Cell> left = ImmutableList.of(value('a'), value('c'), value('e'));
        List<Cell> right = ImmutableList.of(value('b'), value('d'));
        assertThat(collate(left.iterator(), right.iterator()))
                .containsExactly(value('a'), value('b'), value('c'), value('d'), value('e'));
    }

    private static ColumnFamily newCF() {
        return ArrayBackedSortedColumns.factory.create(metadata);
    }

    private static List<Cell> collate(Iterator<? extends OnDiskAtom>... cells) {
        return collate(newCF(), cells);
    }

    private static List<Cell> collate(ColumnFamily returnCf, Iterator<? extends OnDiskAtom>... cells) {
        IDiskAtomFilter filter = new SliceQueryFilter(ColumnSlice.ALL_COLUMNS, false, Integer.MAX_VALUE);
        QueryFilter.collateOnDiskAtom(returnCf, Arrays.asList(cells), filter, null, WRITE_TIME + 1, 10_000);
        return read(returnCf);
    }

    private static List<Cell> read(ColumnFamily cf) {
        DeletionInfo.InOrderTester tester = cf.inOrderDeletionTester();
        List<Cell> result = new ArrayList<>();
        for (Cell cell : cf) {
            if (!tester.isDeleted(cell)) {
                result.add(cell);
            }
        }
        return result;
    }

    @Test
    public void testReconcileDuplicates() {
        assertThat(reconcileDuplicates(newCF(), value('a'), value('b', 'y', 123), value('b', 'n', 122), value('c')))
                .containsExactly(value('a'), value('b', 'y', 123), value('c'));
    }

    @Test
    public void testReconcileDuplicates_singleCell() {
        assertThat(reconcileDuplicates(newCF(), value('a'))).containsExactly(value('a'));
    }

    @Test
    public void testReconcileDuplicates_gathersTombstones() {
        ColumnFamily cf = newCF();
        assertThat(reconcileDuplicates(cf, value('a'), rangeDelete('b', 'c'), value('d')))
                .containsExactly(value('a'), value('d'));
        assertThat(cf.deletionInfo().rangeIterator()).containsExactly(rangeDelete('b', 'c'));
    }

    @Test
    public void testFilterTombstones_tombstone_skipped_if_next_tombstone_does_not_overlap() {
        assertThat(filter(rangeDelete('a', 'b'), rangeDelete('c', 'd'), value('c'))).isEmpty();
    }

    @Test
    public void testFilterTombstones_tombstone_skipped_if_next_cell_does_not_overlap() {
        Cell value = value('c');
        Cell unsortedOverlappingToEnsureWeClearedTombstone = value('a');
        assertThat(filter(rangeDelete('a', 'b'), value, unsortedOverlappingToEnsureWeClearedTombstone))
            .containsExactly(value, unsortedOverlappingToEnsureWeClearedTombstone);
    }

    @Test
    public void testFilterTombstones_never_returns_tombstones_out_of_order() {
        OnDiskAtom[] cells = new OnDiskAtom[] { rangeDelete('a', 'c', 123), value('a', 124) };
        assertThat(filter(cells)).containsExactly(cells);
    }

    @Test
    public void testFilterTombstones_skips_cell_if_overlapped_by_current_tombstone() {
        assertThat(filter(rangeDelete('a', 'b'), value('a'))).isEmpty();
    }

    @Test
    public void testFilterTombstones_does_not_skip_overlapping_cell_if_cell_newer_than_tombstone() {
        Cell value = value('a', TOMBSTONES_TS);
        assertThat(filter(rangeDelete('a', 'b'), value)).contains(value);
    }

    @Test
    public void testFilterTombstones_coalesces_tombstones_that_supercede() {
        assertThat(filter(rangeDelete('a', 'd'), rangeDelete('b', 'c'), rangeDelete('b', 'd', TOMBSTONES_TS + 1)))
                .containsExactly(rangeDelete('a', 'd'));
    }

    @Test
    public void testFilterTombstones_does_not_coalesce_when_incompatible_timestamps() {
        assertThat(filter(rangeDelete('a', 'd'), rangeDelete('b', 'c', TOMBSTONES_TS + 1), rangeDelete('b', 'd', TOMBSTONES_TS + 2)))
                .containsExactly(rangeDelete('a', 'd'), rangeDelete('b', 'c', TOMBSTONES_TS + 1));
    }

    @Test
    public void testFilterTombstones_returns_range_tombstones_that_are_not_droppable() {
        RangeTombstone tombstone = nonDroppableRangeDelete('a', 'b');
        assertThat(filter(tombstone, value('a'), rangeDelete('d', 'f'))).containsExactly(tombstone);
    }

    @Test
    public void testFilterTombstones_returns_pending_last_tombstone_if_not_droppable() {
        RangeTombstone tombstone = nonDroppableRangeDelete('a', 'c');
        assertThat(filter(tombstone)).containsExactly(tombstone);
    }

    private static List<Cell> reconcileDuplicates(ColumnFamily returnCF, OnDiskAtom... cells) {
        return ImmutableList.copyOf(QueryFilter.reconcileDuplicatesAndGatherTombstones(
                returnCF,
                metadata.comparator.columnComparator(false),
                Arrays.asList(cells).iterator()));
    }

    // add a bunch of cells either side to check for edge cases
    private static List<OnDiskAtom> filter(OnDiskAtom... atoms) {
        int buffering = 25;
        List<OnDiskAtom> enhanced = new ArrayList<>(buffering + atoms.length + buffering);
        for (int i = 0; i < 25; i++) {
            enhanced.add(value((char) i));
        }
        for (int i = 0; i < atoms.length; i++) {
            enhanced.add(atoms[i]);
        }
        for (int i = 0; i < 25; i++) {
            enhanced.add(value((char) ('z' + i)));
        }
        List<OnDiskAtom> result = ImmutableList.copyOf(QueryFilter.filterTombstones(metadata.comparator, enhanced.iterator(), WRITE_TIME + 1));
        return result.subList(buffering, result.size() - buffering);
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

    private static Cell delete(char key, long timestamp, int markedForDeletionAt) {
        return new BufferDeletedCell(key(key), markedForDeletionAt, timestamp);
    }

    private static RangeTombstone rangeDelete(char from, char to) {
        return new RangeTombstone(key(from), key(to), TOMBSTONES_TS, WRITE_TIME);
    }

    private static RangeTombstone rangeDelete(char from, char to, long timestamp) {
        return new RangeTombstone(key(from), key(to), timestamp, WRITE_TIME);
    }

    private static RangeTombstone nonDroppableRangeDelete(char from, char to) {
        return new RangeTombstone(key(from), key(to), TOMBSTONES_TS, WRITE_TIME + 2);
    }

    private static CellName key(char character) {
        return (CellName) type.fromByteBuffer(ByteBufferUtil.bytes(String.valueOf(character)));
    }
}
