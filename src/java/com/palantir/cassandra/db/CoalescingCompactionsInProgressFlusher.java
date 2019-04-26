package com.palantir.cassandra.db;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.utils.FBUtilities;

import com.google.common.base.Throwables;

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
public class CoalescingCompactionsInProgressFlusher {
    public static CoalescingCompactionsInProgressFlusher INSTANCE = new CoalescingCompactionsInProgressFlusher();
    
    private volatile CompletableFuture<ReplayPosition> nextResult = new CompletableFuture<ReplayPosition>();
    private final Lock fairLock = new ReentrantLock(true);

    private CoalescingCompactionsInProgressFlusher() { }

    public ReplayPosition forceBlockingFlush() {
        CompletableFuture<ReplayPosition> future = nextResult;

        completeOrWaitForCompletion(future);

        return getResult(future);
    }

    private void completeOrWaitForCompletion(CompletableFuture<ReplayPosition> future) {
        fairLock.lock();
        try {
            resetAndCompleteIfNotCompleted(future);
        } finally {
            fairLock.unlock();
        }
    }

    private void resetAndCompleteIfNotCompleted(CompletableFuture<ReplayPosition> future) {
        if (future.isDone()) {
            return;
        }

        nextResult = new CompletableFuture<ReplayPosition>();
        try {
            future.complete(FBUtilities.waitOnFuture(
                    Keyspace.open(SystemKeyspace.NAME)
                            .getColumnFamilyStore(SystemKeyspace.COMPACTIONS_IN_PROGRESS)
                            .forceFlush()));
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
    }

    private ReplayPosition getResult(CompletableFuture<ReplayPosition> future) {
        try {
            return future.getNow(null);
        } catch (CompletionException ex) {
            throw Throwables.propagate(ex.getCause());
        }
    }
}
