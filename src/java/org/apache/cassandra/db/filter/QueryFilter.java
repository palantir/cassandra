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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import org.apache.cassandra.FilterExperiment;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.columniterator.IdentityQueryFilter;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.utils.MergeIterator;

public class QueryFilter
{
    public final DecoratedKey key;
    public final String cfName;
    public final IDiskAtomFilter filter;
    public final long timestamp;

    public QueryFilter(DecoratedKey key, String cfName, IDiskAtomFilter filter, long timestamp)
    {
        this.key = key;
        this.cfName = cfName;
        this.filter = filter;
        this.timestamp = timestamp;
    }

    public Iterator<Cell> getIterator(ColumnFamily cf)
    {
        assert cf != null;
        return filter.getColumnIterator(cf);
    }

    public OnDiskAtomIterator getSSTableColumnIterator(SSTableReader sstable)
    {
        return filter.getSSTableColumnIterator(sstable, key);
    }

    public void collateOnDiskAtom(ColumnFamily returnCF,
                                  List<? extends Iterator<? extends OnDiskAtom>> toCollate,
                                  int gcBefore, FilterExperiment experiment)
    {
        collateOnDiskAtom(returnCF, toCollate, filter, this.key, gcBefore, timestamp, experiment);
    }

    /**
     * Palantir: In principle, this cache should not really have to care about the row cache. This is more a
     * safety concern than anything else - the row cache makes many more assumptions about what's returned, and
     * we are explicitly breaking some assumptions. So given that we don't actually use the row cache, let's just
     * deoptimize if it's on.
     */
    private static boolean isRowCacheEnabled(ColumnFamily cf) {
        return cf.metadata().getCaching().rowCache.isEnabled() && CacheService.instance.rowCache.getCapacity() > 0;
    }

    /**
     * Palantir: What we found in practice was that the vanilla version of this function can cause OOMs.
     * Default Cassandra keeps range tombstones separate from the values that they tombstone and due to the
     * order in which they are added to the internal data structures, must keep them for the entire duration
     * of reading a row. What this means for us is that if we have a row with many range tombstones,
     * we must hold the entire set of range tombstones in memory for the duration of the read. This cannot be
     * controlled via things like limits, and so you can end up with rows that simply cannot be read, or
     * that slow down Cassandra to the point of massive garbage collection overheads.
     *
     * Here we attempt to handle range tombstones in a constant space mechanism, by merging iterators beforehand
     * and dropping unnecessary cells and range tombstones at that point. So while the data must still all be
     * read, it need not be read into memory, which increases overall server stability.
     *
     * We skip this optimization if either the row cache is in use (because the row cache requires that this codepath
     * not drop droppable tombstones (see comments in CollationController describing how removeDeletedCF must be handled
     * above that level)) or we're scanning in reverse, in which case this does not work. No worries, internally we use
     * neither. It's probably actually safe to do the row cache version of this (I expect it's a concurrency concern).
     *
     * Stuff we thought about here:
     * - It's not safe to do this in the way we do if a range tombstone starting from  X is sorted after a later cell
     *   written at X. But this is guaranteed by the onDiskAtomComparator (which is used by compactions in LazilyCompactedRow).
     * - We can only safely get rid of range tombstones that are droppable. If they are not droppable, they break some
     *   Cassandra consistency guarantees - consider how a node running this optimization would behave when interoperating
     *   with a node not running the optimization - read repair would go wild. It does not affect us, because we only
     *   write range tombstones at consistency ALL, but it's still a C* correctness concern and we don't know where
     *   they write range tombstones internally. In practice then, this method just does most of the prework of
     *   ColumnFamilyStore.removeDeleted.
     * - Tombstones that we've dropped will no longer cover cells that are stored in the memtable, were that the case.
     *   This is not a problem for us - we only drop droppable tombstones and so it'd be correct for a compaction to
     *   simply delete the tombstones.
     * - Cassandra does not always store its data in order on-disk. Notably it might repeat range tombstones, to ensure that
     *   tombstones that are wider than index entries still get read (in practice this won't happen for us). This
     *   transformation is safe for that case.
     */
    public static void collateOnDiskAtom(ColumnFamily returnCF,
                                         List<? extends Iterator<? extends OnDiskAtom>> toCollate,
                                         IDiskAtomFilter filter,
                                         DecoratedKey key,
                                         int gcBefore,
                                         long timestamp,
                                         FilterExperiment experiment)
    {
        if (experiment == FilterExperiment.USE_LEGACY || filter.isReversed() || isRowCacheEnabled(returnCF)) {
            legacyCollateOnDiskAtom(returnCF, toCollate, filter, key, gcBefore, timestamp);
        } else {
            optimizedCollateOnDiskAtom(returnCF, toCollate, filter, key, gcBefore, timestamp);
        }
    }

    /**
     * Palantir: this deoptimization is _super lame_ but necessary in order for the optimized code
     * to match the semantics of the unoptimized code exactly. What's happening is that because
     * the original code is roughly nested merge(gatherTombstones(iterators)), whereas ours is
     * nested gatherTombstones(merge(iterators)), if the next element past the last one we read
     * is a tombstone (or sequence of tombstones), they'll
     * all be gathered at that point into the return cf, regardless of whether they are ever
     * consumed from the merge iterator.
     * This is silly, because it means that if we're merging (a, b, c, d) and (rangeDelete(f, h))
     * with limit 1, then our result cf will contain (a, rangeDelete(f, h)) despite the tombstone
     * being discontinuous. And it means that if we have a lot of tombstones and we read the latest
     * entry only, we will read all of the tombstones nonetheless.
     * We can save some of the stress by not adding droppable tombstones
     * to the return c.f. (they'll be early removed anyhow). We can't just throw them away,
     * because they could be tombstoning other cells.
     *
     * This deoptimization should only exist in the a/b comparison phase of rolling this out
     * - we can kill it once that passes. The consequence of killing it is that while we roll
     * nodes onto the version without this deoptimization, their query results may not match
     * exactly. Empirically, this means that 0.5% of queries will do a full (and unnecessary)
     * repair for the duration of the roll. The good news is that the repairing will write
     * such tombstones into the memtable, and so this can only happen once per row because
     * memtable DeletionInfo is added directly to returnCF rather than being present in its
     * iterator.
     */
    private static List<Iterator<OnDiskAtom>> handleFirstEntryRangeTombstone(
            ColumnFamily returnCF,
            List<? extends Iterator<? extends OnDiskAtom>> iterators,
            int gcBefore) {
        List<Iterator<OnDiskAtom>> ret = new ArrayList<>(iterators.size());
        for (Iterator<? extends OnDiskAtom> iterator : iterators) {
            PeekingIterator<OnDiskAtom> peeking = Iterators.peekingIterator(iterator);
            ret.add(new AbstractIterator<OnDiskAtom>()
            {
                private final Deque<RangeTombstone> buffer = new ArrayDeque<>(0);

                protected OnDiskAtom computeNext()
                {
                    if (!buffer.isEmpty()) {
                        return buffer.remove();
                    }
                    while (peeking.hasNext() && peeking.peek() instanceof RangeTombstone) {
                        RangeTombstone tombstone = (RangeTombstone) peeking.next();
                        if (isDroppable(tombstone, gcBefore)) {
                            buffer.add(tombstone);
                        } else {
                            returnCF.addAtom(tombstone);
                        }
                    }
                    if (!buffer.isEmpty()) {
                        return buffer.remove();
                    } else if (peeking.hasNext()) {
                        return peeking.next();
                    } else {
                        return endOfData();
                    }
                }
            });
        }
        return ret;
    }

    private static boolean isDroppable(RangeTombstone tombstone, int gcBefore) {
        return tombstone.getLocalDeletionTime() < gcBefore;
    }

    private static void optimizedCollateOnDiskAtom(ColumnFamily returnCF,
                                                  List<? extends Iterator<? extends OnDiskAtom>> toCollate,
                                                  IDiskAtomFilter filter,
                                                  DecoratedKey key,
                                                  int gcBefore,
                                                  long timestamp) {
        List<Iterator<OnDiskAtom>> iterators = handleFirstEntryRangeTombstone(returnCF, toCollate, gcBefore);
        Iterator<OnDiskAtom> merged = merge(returnCF.getComparator(), iterators);
        Iterator<OnDiskAtom> filtered = filterTombstones(returnCF.getComparator(), merged, gcBefore);
        Iterator<Cell> reconciled = reconcileDuplicatesAndGatherTombstones(
            returnCF, filter.getColumnComparator(returnCF.getComparator()), filtered);
        filter.collectReducedColumns(returnCF, reconciled, key, gcBefore, timestamp);
    }

    private static void legacyCollateOnDiskAtom(ColumnFamily returnCF,
                                                List<? extends Iterator<? extends OnDiskAtom>> toCollate,
                                                IDiskAtomFilter filter,
                                                DecoratedKey key,
                                                int gcBefore,
                                                long timestamp)
    {
        List<Iterator<Cell>> filteredIterators = new ArrayList<>(toCollate.size());
        for (Iterator<? extends OnDiskAtom> iter : toCollate)
            filteredIterators.add(gatherTombstones(returnCF, iter));
        collateColumns(returnCF, filteredIterators, filter, key, gcBefore, timestamp);
    }

    /**
     * Serves the same purpose as the MergeIterator.Reducer kept in this class, except it works properly for a
     * single iterator.
     *
     * Also gathers tombstones in the same way as gatherTombstones. We conceptually want to nest reconcile(gather(merge(
     * whereas stock Cassandra nests reconcile(merge(gather(, so if we are merging ('a', rangeDelete('b', ?)) and
     * ('c', rangeDelete('d')) then if we compose in that fashion, we would merge to ('a', rangeDelete('b', ?), 'c')
     * and then try to reconcile 'a', which requires gathering all tombstones up to the next live cell, 'c').
     * Meanwhile, stock cassandra would be conceptually merging with 'c' and not need to read the tombstone in 'a'.
     * So, we merge the two steps, and stop reconciling as soon as we see something that is not a cell. This is
     * additionally beneficial since it means we do strictly less work.
     */
    @VisibleForTesting
    static Iterator<Cell> reconcileDuplicatesAndGatherTombstones(
            ColumnFamily returnCF, Comparator<Cell> comparator, Iterator<? extends OnDiskAtom> atoms) {
        final PeekingIterator<OnDiskAtom> peeking = Iterators.peekingIterator(atoms);
        return new AbstractIterator<Cell>() {
            protected Cell computeNext()
            {
                OnDiskAtom atom = null;
                while (peeking.hasNext() && (atom = peeking.next()) instanceof RangeTombstone) {
                    returnCF.addAtom(atom);
                    atom = null;
                }
                if (atom == null) {
                    return endOfData();
                }
                Cell root = (Cell) atom;
                while (peeking.hasNext()
                       && peeking.peek() instanceof Cell
                       && comparator.compare(root, (Cell) peeking.peek()) == 0) {
                    root = root.reconcile((Cell) peeking.next());
                }
                return root;
            }
        };
    }

    // Palantir: This is only used for the names query filter, which we don't use so don't optimize.
    // When there is only a single source of atoms, we can skip the collate step
    public void collateOnDiskAtom(ColumnFamily returnCF, Iterator<? extends OnDiskAtom> toCollate, int gcBefore)
    {
        filter.collectReducedColumns(
            returnCF,
            gatherTombstones(returnCF, toCollate),
            this.key,
            gcBefore,
            timestamp);
    }

    public void collateColumns(ColumnFamily returnCF, List<? extends Iterator<Cell>> toCollate, int gcBefore)
    {
        collateColumns(returnCF, toCollate, filter, this.key, gcBefore, timestamp);
    }

    public static void collateColumns(ColumnFamily returnCF,
                                      List<? extends Iterator<Cell>> toCollate,
                                      IDiskAtomFilter filter,
                                      DecoratedKey key,
                                      int gcBefore,
                                      long timestamp)
    {
        Comparator<Cell> comparator = filter.getColumnComparator(returnCF.getComparator());

        Iterator<Cell> reduced = toCollate.size() == 1
                               ? toCollate.get(0)
                               : MergeIterator.get(toCollate, comparator, getReducer(comparator));

        filter.collectReducedColumns(returnCF, reduced, key, gcBefore, timestamp);
    }

    private static Iterator<OnDiskAtom> merge(CellNameType comparator, List<? extends Iterator<? extends OnDiskAtom>> iterators) {
        return Iterators.mergeSorted(iterators, comparator.onDiskAtomComparator());
    }

    /**
     * A complex function which holds a RangeTombstone before emitting it. Uses the held range tombstone
     * to elide emitting cells that are covered by it, or redundant other range tombstones. Emits the range
     * tombstone iff an incompatible range tombstone is seen, otherwise skips.
     */
    @VisibleForTesting
    static Iterator<OnDiskAtom> filterTombstones(
            final CellNameType comparator, Iterator<? extends OnDiskAtom> backingIterator, int gcBefore) {
        final PeekingIterator<OnDiskAtom> peeking = Iterators.peekingIterator(backingIterator);
        return new AbstractIterator<OnDiskAtom>()
        {
            RangeTombstone maybePendingRangeTombstone;

            private RangeTombstone getPendingRangeTombstoneAndSet(RangeTombstone newValue) {
                RangeTombstone present = maybePendingRangeTombstone;
                maybePendingRangeTombstone = newValue;
                return present;
            }

            protected OnDiskAtom computeNext()
            {
                while (peeking.hasNext()) {
                    OnDiskAtom nextAtom = peeking.peek();
                    // if the next atom is outside of the range, we can dump any cached range tombstone that haven't
                    // deleted anything.
                    if (maybePendingRangeTombstone == null
                            || !maybePendingRangeTombstone.includes(comparator, nextAtom.name())) {
                        // If the range tombstone is not droppable, we must return it.
                        if (pendingTombstoneNotDroppable()) {
                            return getPendingRangeTombstoneAndSet(null);
                        }

                        if (nextAtom instanceof RangeTombstone) {
                            maybePendingRangeTombstone = (RangeTombstone) peeking.next();
                            continue;
                        } else {
                            maybePendingRangeTombstone = null;
                            return peeking.next();
                        }
                    }
                    // we have a range tombstone, we're in the range
                    if (nextAtom instanceof Cell) {
                        // if the next cell is overwritten by the range tombstone, skip it
                        if (nextAtom.timestamp() < maybePendingRangeTombstone.timestamp()) {
                            peeking.next();
                            continue;
                        } else {
                            // must return to make sure we never switch orders of cells in output - we filter only.
                            return getPendingRangeTombstoneAndSet(null);
                        }
                    } else {
                        // we have an overlap. We expect that the present tombstone
                        // will supercede the old, based on how we write...
                        RangeTombstone tombstone = (RangeTombstone) nextAtom;
                        if (maybePendingRangeTombstone.supersedes(tombstone, comparator)) {
                            peeking.next();
                            continue;
                        } else {
                            // cannot optimize, return, move on
                            return getPendingRangeTombstoneAndSet((RangeTombstone) peeking.next());
                        }
                    }
                }
                if (pendingTombstoneNotDroppable()) {
                    return getPendingRangeTombstoneAndSet(null);
                }
                return endOfData();
            }

            private boolean pendingTombstoneNotDroppable() {
                return maybePendingRangeTombstone != null && maybePendingRangeTombstone.getLocalDeletionTime() >= gcBefore;
            }
        };
    }

    private static MergeIterator.Reducer<Cell, Cell> getReducer(final Comparator<Cell> comparator)
    {
        // define a 'reduced' iterator that merges columns w/ the same name, which
        // greatly simplifies computing liveColumns in the presence of tombstones.
        return new MergeIterator.Reducer<Cell, Cell>()
        {
            Cell current;

            public void reduce(Cell next)
            {
                assert current == null || comparator.compare(current, next) == 0;
                current = current == null ? next : current.reconcile(next);
            }

            protected Cell getReduced()
            {
                assert current != null;
                Cell toReturn = current;
                current = null;
                return toReturn;
            }

            @Override
            public boolean trivialReduceIsTrivial()
            {
                return true;
            }
        };
    }

    /**
     * Given an iterator of on disk atom, returns an iterator that filters the tombstone range
     * markers adding them to {@code returnCF} and returns the normal column.
     */
    public static Iterator<Cell> gatherTombstones(final ColumnFamily returnCF, final Iterator<? extends OnDiskAtom> iter)
    {
        return new Iterator<Cell>()
        {
            private Cell next;

            public boolean hasNext()
            {
                if (next != null)
                    return true;

                getNext();
                return next != null;
            }

            public Cell next()
            {
                if (next == null)
                    getNext();

                assert next != null;
                Cell toReturn = next;
                next = null;
                return toReturn;
            }

            private void getNext()
            {
                while (iter.hasNext())
                {
                    OnDiskAtom atom = iter.next();

                    if (atom instanceof Cell)
                    {
                        next = (Cell)atom;
                        break;
                    }
                    else
                    {
                        returnCF.addAtom(atom);
                    }
                }
            }

            public void remove()
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    public String getColumnFamilyName()
    {
        return cfName;
    }

    /**
     * @return a QueryFilter object to satisfy the given slice criteria:
     * @param key the row to slice
     * @param cfName column family to query
     * @param start column to start slice at, inclusive; empty for "the first column"
     * @param finish column to stop slice at, inclusive; empty for "the last column"
     * @param reversed true to start with the largest column (as determined by configured sort order) instead of smallest
     * @param limit maximum number of non-deleted columns to return
     * @param timestamp time to use for determining expiring columns' state
     */
    public static QueryFilter getSliceFilter(DecoratedKey key,
                                             String cfName,
                                             Composite start,
                                             Composite finish,
                                             boolean reversed,
                                             int limit,
                                             long timestamp)
    {
        return new QueryFilter(key, cfName, new SliceQueryFilter(start, finish, reversed, limit), timestamp);
    }

    /**
     * return a QueryFilter object that includes every column in the row.
     * This is dangerous on large rows; avoid except for test code.
     */
    public static QueryFilter getIdentityFilter(DecoratedKey key, String cfName, long timestamp)
    {
        return new QueryFilter(key, cfName, new IdentityQueryFilter(), timestamp);
    }

    /**
     * @return a QueryFilter object that will return columns matching the given names
     * @param key the row to slice
     * @param cfName column family to query
     * @param columns the column names to restrict the results to, sorted in comparator order
     */
    public static QueryFilter getNamesFilter(DecoratedKey key, String cfName, SortedSet<CellName> columns, long timestamp)
    {
        return new QueryFilter(key, cfName, new NamesQueryFilter(columns), timestamp);
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "(key=" + key + ", cfName=" + cfName + (filter == null ? "" : ", filter=" + filter) + ")";
    }

    public boolean shouldInclude(SSTableReader sstable)
    {
        return filter.shouldInclude(sstable);
    }

    public void delete(DeletionInfo target, ColumnFamily source)
    {
        target.add(source.deletionInfo().getTopLevelDeletion());
        // source is the CF currently in the memtable, and it can be large compared to what the filter selects,
        // so only consider those range tombstones that the filter do select.
        for (Iterator<RangeTombstone> iter = filter.getRangeTombstoneIterator(source); iter.hasNext(); )
            target.add(iter.next(), source.getComparator());
    }
}
