/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.cassandra.db;

import java.util.function.Supplier;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.utils.FBUtilities;

import com.palantir.common.concurrent.CoalescingSupplier;

/**
 * Executing a single compaction task requires two flushes of the system.compactions_in_progress table: at start and
 * finish. These flushes are blocking and a forced flush itself is synchronized so only one flush can happen at a time.
 * On nodes with considerable compaction task throughput and concurrency, these flushes pose a number of problems:
 * - they needlessly bottleneck the compaction rate by having to wait on all flushes to occur serially despite the
 *   compactions themselves running in parallel
 * - they generate lots of very tiny sstables in the system.compactions_in_progress table that in turn must be
 *   compacted together and thus further contribute to this issue
 *   
 * The rest of this explanation operates under the assumption that these blocking flushes are indeed required, and that
 * they exist to ensure that the data written immediately before the forceBlockingFlush exists in an sstable on disk
 * before continuing on to the next phase of the compaction.
 * 
 * When two independent compactions A and B both execute a write to this table and then call the forceBlockingFlush, if
 * both A and B have fully completed their write before either has attempted to run their flush, we can conclude that
 * either tasks flush will have the affect of flushing the other tasks data as well. In this case we need only execute
 * one flush, not two.
 * 
 * This class attempts to abstract away calling forceBlockingFlush on the system.compactions_in_progress table and
 * transparently tracking and managing when and how flushes occur in order to minimize the number of flushes actually
 * executed without altering any of the guarantees supplied by the forceBlockingFlush method.
 * 
 * @author tpetracca
 */
public class CompactionsInProgressFlusher {
    public static final CompactionsInProgressFlusher INSTANCE = new CompactionsInProgressFlusher();
    
    private final Supplier<ReplayPosition> flusher = () -> FBUtilities.waitOnFuture(
            Keyspace.open(SystemKeyspace.NAME)
                    .getColumnFamilyStore(SystemKeyspace.COMPACTIONS_IN_PROGRESS)
                    .forceFlush());
    private final Supplier<ReplayPosition> coalescingFlusher = new CoalescingSupplier<ReplayPosition>(flusher);
    
    private CompactionsInProgressFlusher() { }
    
    public ReplayPosition forceBlockingFlush() {
        if (Boolean.getBoolean("palantir_cassandra.coalesce_cip_flushes")) {
            return coalescingFlusher.get();
        }
        return flusher.get();
    }
}
