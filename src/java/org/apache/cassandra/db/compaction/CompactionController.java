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
package org.apache.cassandra.db.compaction;

import java.util.*;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;

import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Memtable;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.AlwaysPresentFilter;

import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.OverlapIterator;
import org.apache.cassandra.utils.concurrent.Refs;

import static org.apache.cassandra.db.lifecycle.SSTableIntervalTree.buildIntervals;

/**
 * Manage compaction options.
 */
public class CompactionController implements AutoCloseable
{
    private static final Logger logger = LoggerFactory.getLogger(CompactionController.class);
    static final boolean NEVER_PURGE_TOMBSTONES = Boolean.getBoolean("cassandra.never_purge_tombstones");

    public final ColumnFamilyStore cfs;
    // note that overlapIterator and overlappingSSTables will be null if NEVER_PURGE_TOMBSTONES is set - this is a
    // good thing so that noone starts using them and thinks that if overlappingSSTables is empty, there
    // is no overlap.
    private Refs<SSTableReader> overlappingSSTables;
    private OverlapIterator<RowPosition, SSTableReader> overlapIterator;
    private final Iterable<SSTableReader> compacting;

    public final int gcBefore;

    protected CompactionController(ColumnFamilyStore cfs, int maxValue)
    {
        this(cfs, null, maxValue);
    }

    public CompactionController(ColumnFamilyStore cfs, Set<SSTableReader> compacting, int gcBefore)
    {
        assert cfs != null;
        this.cfs = cfs;
        this.gcBefore = gcBefore;
        this.compacting = compacting;
        refreshOverlaps();
        if (NEVER_PURGE_TOMBSTONES)
            logger.warn("You are running with -Dcassandra.never_purge_tombstones=true, this is dangerous!");
    }

    void maybeRefreshOverlaps()
    {
        if (doNotPurgeTombstones(cfs.keyspace.getName()))
        {
            logger.debug("not refreshing overlaps - doNotPurgeTombstones returned true for keyspace {}", cfs.keyspace.getName());
            return;
        }

        for (SSTableReader reader : overlappingSSTables)
        {
            if (reader.isMarkedCompacted())
            {
                refreshOverlaps();
                return;
            }
        }
    }

    private void refreshOverlaps()
    {
        if (doNotPurgeTombstones(cfs.keyspace.getName()))
        {
            logger.debug("not refreshing overlaps - doNotPurgeTombstones returned true for keyspace {}", cfs.keyspace.getName());
            return;
        }

        if (this.overlappingSSTables != null)
            overlappingSSTables.release();

        if (compacting == null)
            overlappingSSTables = Refs.tryRef(Collections.<SSTableReader>emptyList());
        else
            overlappingSSTables = cfs.getAndReferenceOverlappingSSTables(compacting);
        this.overlapIterator = new OverlapIterator<>(buildIntervals(overlappingSSTables));
    }

    public Set<SSTableReader> getFullyExpiredSSTables()
    {
        return getFullyExpiredSSTables(cfs, compacting, overlappingSSTables, gcBefore);
    }

    /**
     * Finds expired sstables
     *
     * works something like this;
     * 1. find "global" minTimestamp of overlapping sstables, compacting sstables and memtables containing any non-expired data
     * 2. build a list of fully expired candidates
     * 3. check if the candidates to be dropped actually can be dropped (maxTimestamp < global minTimestamp)
     *    - if not droppable, remove from candidates
     * 4. return candidates.
     *
     * @param cfStore
     * @param compacting we take the drop-candidates from this set, it is usually the sstables included in the compaction
     * @param overlapping the sstables that overlap the ones in compacting.
     * @param gcBefore
     * @return
     */
    public static Set<SSTableReader> getFullyExpiredSSTables(ColumnFamilyStore cfStore, Iterable<SSTableReader> compacting, Iterable<SSTableReader> overlapping, int gcBefore)
    {
        logger.trace("Checking droppable sstables in {}", cfStore);

        if (compacting == null)
            return Collections.<SSTableReader>emptySet();

        if (doNotPurgeTombstones(cfStore.keyspace.getName())) {
            logger.debug("not looking for droppable sstables - doNotPurgeTombstones returned true for keyspace {}", cfStore.keyspace.getName());
            return Collections.<SSTableReader>emptySet();
        }

        List<SSTableReader> candidates = new ArrayList<>();

        long minTimestamp = Long.MAX_VALUE;

        for (SSTableReader sstable : overlapping)
        {
            // Overlapping might include fully expired sstables. What we care about here is
            // the min timestamp of the overlapping sstables that actually contain live data.
            if (sstable.getSSTableMetadata().maxLocalDeletionTime >= gcBefore)
                minTimestamp = Math.min(minTimestamp, sstable.getMinTimestamp());
        }

        for (SSTableReader candidate : compacting)
        {
            if (candidate.getSSTableMetadata().maxLocalDeletionTime < gcBefore)
                candidates.add(candidate);
            else
                minTimestamp = Math.min(minTimestamp, candidate.getMinTimestamp());
        }

        for (Memtable memtable : cfStore.getTracker().getView().getAllMemtables())
            minTimestamp = Math.min(minTimestamp, memtable.getMinTimestamp());

        // At this point, minTimestamp denotes the lowest timestamp of any relevant
        // SSTable or Memtable that contains a constructive value. candidates contains all the
        // candidates with no constructive values. The ones out of these that have
        // (getMaxTimestamp() < minTimestamp) serve no purpose anymore.

        Iterator<SSTableReader> iterator = candidates.iterator();
        while (iterator.hasNext())
        {
            SSTableReader candidate = iterator.next();
            if (candidate.getMaxTimestamp() >= minTimestamp)
            {
                iterator.remove();
            }
            else
            {
               logger.trace("Dropping expired SSTable {} (maxLocalDeletionTime={}, gcBefore={})",
                        candidate, candidate.getSSTableMetadata().maxLocalDeletionTime, gcBefore);
            }
        }
        return new HashSet<>(candidates);
    }

    public String getKeyspace()
    {
        return cfs.keyspace.getName();
    }

    public String getColumnFamily()
    {
        return cfs.name;
    }

    /**
     * @param key
     * @return a predicate for whether tombstones marked for deletion at the given time for the given partition are
     * purgeable; we calculate this by checking whether the deletion time is less than the min timestamp of all SSTables
     * containing his partition and not participating in the compaction. This means there isn't any data in those
     * sstables that might still need to be suppressed by a tombstone at this timestamp.
     */
    public Predicate<Long> getPurgeEvaluator(DecoratedKey key)
    {
        if (doNotPurgeTombstones(getKeyspace()))
        {
            logger.debug("Purge evaluator always returning false - doNotPurgeTombstones returned true for keyspace {}", getKeyspace());
            return Predicates.alwaysFalse();
        }

        overlapIterator.update(key);
        Set<SSTableReader> filteredSSTables = overlapIterator.overlaps();
        Iterable<Memtable> memtables = cfs.getTracker().getView().getAllMemtables();
        long minTimestampSeen = Long.MAX_VALUE;
        boolean hasTimestamp = false;

        for (SSTableReader sstable: filteredSSTables)
        {
            // if we don't have bloom filter(bf_fp_chance=1.0 or filter file is missing),
            // we check index file instead.
            if (sstable.getBloomFilter() instanceof AlwaysPresentFilter && sstable.getPosition(key, SSTableReader.Operator.EQ, false) != null
                || sstable.getBloomFilter().isPresent(key))
            {
                minTimestampSeen = Math.min(minTimestampSeen, sstable.getMinTimestamp());
                hasTimestamp = true;
            }

        }

        for (Memtable memtable : memtables)
        {
            ColumnFamily cf = memtable.getColumnFamily(key);
            if (cf != null)
            {
                minTimestampSeen = Math.min(minTimestampSeen, memtable.getMinTimestamp());
                hasTimestamp = true;
            }
        }

        if (!hasTimestamp)
            return Predicates.alwaysTrue();
        else
        {
            final long finalTimestamp = minTimestampSeen;
            logger.debug("Creating purge evaluator; [maximumPurgeableTimestampExclusive:{},decoratedKey:{}]", finalTimestamp, key); 
            return new Predicate<Long>()
            {
                public boolean apply(Long time)
                {
                    return time < finalTimestamp;
                }
            };
        }
    }

    public void close()
    {
        if (overlappingSSTables != null)
            overlappingSSTables.release();
    }

    /**
     * @param keyspace
     * @return true if this node may be streaming data for this keyspace, or if cassandra.never_purge_tombstones is set
     * If we are streaming, tombstones should not be purged so that we don't pre-maturely purge those that exceed gc_grace_seconds.
     * <p>
     * Note that this node may receive data without streaming, e.g. during a repair.
     */
    private static boolean doNotPurgeTombstones(String keyspace)
    {
        return NEVER_PURGE_TOMBSTONES
                || StorageService.instance.isRebuilding()
                || pendingRangesExistForKeyspace(keyspace);
    }

    /**
     * @param keyspace
     * @return true if this node is currently streaming data as part of a range movement (e.g., bootstrap, decommission, move)
     * If pending ranges exist, tombstones should not be purged so that we don't pre-maturely purge those that exceed gc_grace_seconds
     */
    public static boolean pendingRangesExistForKeyspace(String keyspace)
    {
        return StorageService.instance.getTokenMetadata().getPendingRanges(keyspace, FBUtilities.getBroadcastAddress()).size() > 0;
    }
}
