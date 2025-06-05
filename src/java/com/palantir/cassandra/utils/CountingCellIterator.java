/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.cassandra.utils;

import java.util.Iterator;

import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ExpiringCell;
import org.apache.cassandra.metrics.ColumnFamilyMetrics;
import org.apache.cassandra.utils.MergeIterator;

/**
 * A wrapper around {@link MergeIterator} specifically for {@link Cell} objects and for use in {@link SliceQueryFilter}
 * that counts the number of cells read and processed from disk for the purposes of understanding performance
 * bottlenecks and cluster behavior. Cell types counted are:
 * - live: cells that have real data and are not an expired TTL; this does not represent exact correctness in that
 *         a cell that is considered deleted by a higher level deletion like a row level delete will be marked as
 *         live here because we had to read it.
 * - tombstone: tombstones that have not yet passed their gc_grace_seconds period
 * - droppableTombstone: a tombstone that is gc-able as it is past its gc_grace_seconds period
 * - droppableTtl: a TTL'ed (formerly live) cell that is now gc-able as it is past its time-to-live.
 *
 * @author tpetracca
 */
public class CountingCellIterator implements Iterator<Cell> {
    private final Iterator<Cell> delegate;

    private final ColumnFamilyMetrics metrics;
    /*
     * Given the way SliceQueryFilter is passed these values, per ColumnFamilyStore.getColumnFamily(QueryFilter filter),
     * it is generally expected that gcBefore will be equivalent to { (now / 1000) - gc_grace_seconds }
     */
    private final long gcBeforeSeconds;
    private final long nowMillis;

    private int droppableTombstones = 0;
    private int droppableTtls = 0;
    private int liveCells = 0;
    private int tombstones = 0;

    public static CountingCellIterator wrapIterator(Iterator<Cell> delegate, ColumnFamilyMetrics metrics, long timestamp, long gcBefore) {
        return new CountingCellIterator(delegate, metrics, timestamp, gcBefore);
    }

    private CountingCellIterator(Iterator<Cell> delegate, ColumnFamilyMetrics metrics, long timestamp, long gcBefore) {
        this.delegate = delegate;
        this.metrics = metrics;
        this.gcBeforeSeconds = gcBefore;
        this.nowMillis = timestamp;
    }

    @Override
    public boolean hasNext() {
        return delegate.hasNext();
    }

    @Override
    public Cell next() {
        Cell next = delegate.next();
        count(next);
        return next;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    public int dead() {
        return boundedAdd(boundedAdd(droppableTombstones, droppableTtls), tombstones);
    }

    public int droppableTombstones() {
        return droppableTombstones;
    }

    public int droppableTtls() {
        return droppableTtls;
    }

    public int live() {
        return liveCells;
    }

    public int tombstones() {
        return tombstones;
    }

    private void count(Cell cell) {
        if (cell.isLive(nowMillis)) {
            liveCells++;
        } else if (ExpiringCell.class.isAssignableFrom(cell.getClass())) {
            droppableTtls++;
        } else if (cell.getLocalDeletionTime() < gcBeforeSeconds) {
            if (metrics != null) {
                metrics.droppableTombstones.mark();
            }
            droppableTombstones++;
        } else {
            tombstones++;
        }
    }

    // if overflow then cap at max value.
    private static int boundedAdd(int a, int b) {
        long result = (long) a + b;
        if (result > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }

        return (int) result;
    }
}
