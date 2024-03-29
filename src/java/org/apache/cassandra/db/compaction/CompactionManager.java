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

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.TabularData;

import org.apache.commons.lang3.StringUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.*;
import com.google.common.util.concurrent.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.AutoSavingCache;
import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.compaction.CompactionInfo.Holder;
import org.apache.cassandra.db.index.SecondaryIndexBuilder;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.metrics.CompactionMetrics;
import org.apache.cassandra.repair.Validator;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.*;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.concurrent.Refs;

import static java.util.Collections.singleton;

/**
 * <p>
 * A singleton which manages a private executor of ongoing compactions.
 * </p>
 * Scheduling for compaction is accomplished by swapping sstables to be compacted into
 * a set via Tracker. New scheduling attempts will ignore currently compacting
 * sstables.
 */
public class CompactionManager implements CompactionManagerMBean
{
    public static final String MBEAN_OBJECT_NAME = "org.apache.cassandra.db:type=CompactionManager";
    private static final Logger logger = LoggerFactory.getLogger(CompactionManager.class);
    public static final CompactionManager instance;

    public static final int NO_GC = Integer.MIN_VALUE;
    public static final int GC_ALL = Integer.MAX_VALUE;

    public static final EnumSet<OperationType> STOPPABLE_COMPACTION_TYPES = EnumSet.of(OperationType.COMPACTION,
                                                                                       OperationType.VALIDATION,
                                                                                       OperationType.CLEANUP,
                                                                                       OperationType.SCRUB,
                                                                                       OperationType.INDEX_BUILD);


    // A thread local that tells us if the current thread is owned by the compaction manager. Used
    // by CounterContext to figure out if it should log a warning for invalid counter shards.
    public static final ThreadLocal<Boolean> isCompactionManager = new ThreadLocal<Boolean>()
    {
        @Override
        protected Boolean initialValue()
        {
            return false;
        }
    };

    static
    {
        instance = new CompactionManager();

        MBeanWrapper.instance.registerMBean(instance, MBEAN_OBJECT_NAME);
    }

    private final CompactionExecutor executor = new CompactionExecutor();
    private final CompactionExecutor validationExecutor = new ValidationExecutor();
    private final static CompactionExecutor cacheCleanupExecutor = new CacheCleanupExecutor();

    private final CompactionMetrics metrics = new CompactionMetrics(executor, validationExecutor);
    @VisibleForTesting
    final Multiset<ColumnFamilyStore> compactingCF = ConcurrentHashMultiset.create();

    private final RateLimiter compactionRateLimiter = RateLimiter.create(Double.MAX_VALUE);

    /**
     * Gets compaction rate limiter.
     * Rate unit is bytes per sec.
     *
     * @return RateLimiter with rate limit set
     */
    public RateLimiter getRateLimiter()
    {
        setRate(DatabaseDescriptor.getCompactionThroughputMbPerSec());
        return compactionRateLimiter;
    }

    /**
     * Sets the rate for the rate limiter. When compaction_throughput_mb_per_sec is 0 or node is bootstrapping,
     * this sets the rate to Double.MAX_VALUE bytes per second.
     * @param throughPutMbPerSec throughput to set in mb per second
     */
    public void setRate(final double throughPutMbPerSec)
    {
        double throughput = throughPutMbPerSec * 1024.0 * 1024.0;
        // if throughput is set to 0, throttling is disabled
        if (throughput == 0 || StorageService.instance.isBootstrapMode())
            throughput = Double.MAX_VALUE;
        if (compactionRateLimiter.getRate() != throughput)
            compactionRateLimiter.setRate(throughput);
    }

    /**
     * Call this whenever a compaction might be needed on the given columnfamily.
     * It's okay to over-call (within reason) if a call is unnecessary, it will
     * turn into a no-op in the bucketing/candidate-scan phase.
     */
    public List<Future<?>> submitBackground(final ColumnFamilyStore cfs)
    {
        if (cfs.isAutoCompactionDisabled())
        {
            logger.trace("Autocompaction is disabled");
            return Collections.emptyList();
        }

        /**
         * If a CF is currently being compacted, and there are no idle threads, submitBackground should be a no-op;
         * we can wait for the current compaction to finish and re-submit when more information is available.
         * Otherwise, we should submit at least one task to prevent starvation by busier CFs, and more if there
         * are idle threads stil. (CASSANDRA-4310)
         */
        int count = compactingCF.count(cfs);
        if (count > 0 && executor.getActiveCount() >= executor.getMaximumPoolSize())
        {
            logger.trace("Background compaction is still running for {}.{} ({} remaining). Skipping",
                         cfs.keyspace.getName(), cfs.name, count);
            return Collections.emptyList();
        }

        logger.trace("Scheduling a background task check for {}.{} with {}",
                     cfs.keyspace.getName(),
                     cfs.name,
                     cfs.getCompactionStrategy().getName());

        List<Future<?>> futures = new ArrayList<>(1);
        Future<?> fut = executor.submitIfRunning(new BackgroundCompactionCandidate(cfs), "background task");
        if (!fut.isCancelled())
            futures.add(fut);
        else
            compactingCF.remove(cfs);
        return futures;
    }

    public boolean isCompacting(Iterable<ColumnFamilyStore> cfses)
    {
        for (ColumnFamilyStore cfs : cfses)
            if (!cfs.getTracker().getCompacting().isEmpty())
                return true;
        return false;
    }

    /**
     * Shutdowns both compaction and validation executors, cancels running compaction / validation,
     * and waits for tasks to complete if tasks were not cancelable.
     */
    public void forceShutdown()
    {
        // shutdown executors to prevent further submission
        executor.shutdown();
        validationExecutor.shutdown();
        cacheCleanupExecutor.shutdown();

        // interrupt compactions and validations
        for (Holder compactionHolder : CompactionMetrics.getCompactions())
        {
            compactionHolder.stop();
        }

        // wait for tasks to terminate
        // compaction tasks are interrupted above, so it shuold be fairy quick
        // until not interrupted tasks to complete.
        for (ExecutorService exec : Arrays.asList(executor, validationExecutor, cacheCleanupExecutor))
        {
            try
            {
                if (!exec.awaitTermination(1, TimeUnit.MINUTES))
                    logger.warn("Failed to wait for compaction executors shutdown");
            }
            catch (InterruptedException e)
            {
                logger.error("Interrupted while waiting for tasks to be terminated", e);
            }
        }
    }

    public void finishCompactionsAndShutdown(long timeout, TimeUnit unit) throws InterruptedException
    {
        executor.shutdown();
        executor.awaitTermination(timeout, unit);
    }

    // the actual sstables to compact are not determined until we run the BCT; that way, if new sstables
    // are created between task submission and execution, we execute against the most up-to-date information
    class BackgroundCompactionCandidate implements Runnable
    {
        private final ColumnFamilyStore cfs;

        BackgroundCompactionCandidate(ColumnFamilyStore cfs)
        {
            compactingCF.add(cfs);
            this.cfs = cfs;
        }

        public void run()
        {
            try
            {
                logger.trace("Checking {}.{}", cfs.keyspace.getName(), cfs.name);
                if (!cfs.isValid())
                {
                    logger.trace("Aborting compaction for dropped CF");
                    return;
                }

                AbstractCompactionStrategy strategy = cfs.getCompactionStrategy();
                AbstractCompactionTask task = strategy.getNextBackgroundTask(getDefaultGcBefore(cfs));
                if (task == null)
                {
                    logger.trace("No tasks available");
                    return;
                }
                task.execute(metrics);
            }
            finally
            {
                compactingCF.remove(cfs);
            }
            submitBackground(cfs);
        }
    }

    /**
     * Run an operation over all sstables using jobs threads
     *
     * @param cfs the column family store to run the operation on
     * @param operation the operation to run
     * @param jobs the number of threads to use - 0 means use all available. It never uses more than concurrent_compactors threads
     * @return status of the operation
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @SuppressWarnings("resource")
    private AllSSTableOpStatus parallelAllSSTableOperation(final ColumnFamilyStore cfs, final OneSSTableOperation operation, int jobs, OperationType operationType) throws ExecutionException, InterruptedException
    {
        List<LifecycleTransaction> transactions = new ArrayList<>();
        try (LifecycleTransaction compacting = cfs.markAllCompacting(operationType))
        {
            Iterable<SSTableReader> sstables = compacting != null ? Lists.newArrayList(operation.filterSSTables(compacting)) : Collections.<SSTableReader>emptyList();
            if (Iterables.isEmpty(sstables))
            {
                logger.info("No sstables for {}.{}", cfs.keyspace.getName(), cfs.name);
                return AllSSTableOpStatus.SUCCESSFUL;
            }

            List<Future<?>> futures = new ArrayList<>();

            for (final SSTableReader sstable : sstables)
            {
                final LifecycleTransaction txn = compacting.split(singleton(sstable));
                transactions.add(txn);
                Callable<Object> callable = new Callable<Object>()
                {
                    @Override
                    public Object call() throws Exception
                    {
                        operation.execute(txn);
                        return this;
                    }
                };
                Future<?> fut = executor.submitIfRunning(callable, "paralell sstable operation");
                if (!fut.isCancelled())
                    futures.add(fut);
                else
                    return AllSSTableOpStatus.ABORTED;

                if (jobs > 0 && futures.size() == jobs)
                {
                    FBUtilities.waitOnFutures(futures);
                    futures.clear();
                }
            }
            FBUtilities.waitOnFutures(futures);
            assert compacting.originals().isEmpty();
            return AllSSTableOpStatus.SUCCESSFUL;
        }
        finally
        {
            Throwable fail = Throwables.close(null, transactions);
            if (fail != null)
                logger.error("Failed to cleanup lifecycle transactions {}", fail);
        }
    }

    /**
     * Run an operation over all sstables using jobs threads
     *
     * @param cfs the column family store to run the operation on
     * @param callable the callable operation to run
     * @param jobs the number of threads to use - 0 means use all available. It never uses more than concurrent_compactors threads
     * @return status of the operation
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @SuppressWarnings("resource")
    private <T> Optional<List<T>> parallelAllSSTableCallable(final ColumnFamilyStore cfs, final OneSSTableCallable<T> callable, int jobs, OperationType operationType) throws ExecutionException, InterruptedException
    {
        List<LifecycleTransaction> transactions = new ArrayList<>();
        try (LifecycleTransaction compacting = cfs.markAllCompacting(operationType))
        {
            Iterable<SSTableReader> sstables = compacting != null ? Lists.newArrayList(callable.filterSSTables(compacting)) : Collections.<SSTableReader>emptyList();
            if (Iterables.isEmpty(sstables))
            {
                logger.info("No sstables for {}.{}", cfs.keyspace.getName(), cfs.name);
                return Optional.of(ImmutableList.of());
            }

            List<Future<T>> futures = new ArrayList<>();

            for (final SSTableReader sstable : sstables)
            {
                final LifecycleTransaction txn = compacting.split(singleton(sstable));
                transactions.add(txn);
                Callable<T> callableGeneric = new Callable<T>()
                {
                    @Override
                    public T call() throws Exception
                    {
                        return callable.execute(txn);
                    }
                };
                Future<T> fut = (ListenableFuture<T>) executor.submitIfRunning(callableGeneric, "parallel sstable callable operation");
                if (!fut.isCancelled())
                    futures.add(fut);
                else
                    return Optional.empty();

                if (jobs > 0 && futures.size() == jobs)
                {
                    FBUtilities.waitOnFutures(futures);
                    futures.clear();
                }
            }
            List<T> results = FBUtilities.waitOnFutures(futures);
            assert compacting.originals().isEmpty();
            return Optional.of(results);
        }
        finally
        {
            Throwable fail = Throwables.close(null, transactions);
            if (fail != null)
                logger.error("Failed to cleanup lifecycle transactions {}", fail);
        }
    }

    private static interface OneSSTableOperation
    {
        Iterable<SSTableReader> filterSSTables(LifecycleTransaction transaction);
        void execute(LifecycleTransaction input) throws IOException;
    }

    private static interface OneSSTableCallable<T>
    {
        Iterable<SSTableReader> filterSSTables(LifecycleTransaction transaction);
        T execute(LifecycleTransaction input) throws IOException;
    }

    public enum AllSSTableOpStatus { ABORTED(1), SUCCESSFUL(0);
        public final int statusCode;

        AllSSTableOpStatus(int statusCode)
        {
            this.statusCode = statusCode;
        }
    }

    public AllSSTableOpStatus performScrub(final ColumnFamilyStore cfs, final boolean skipCorrupted, final boolean checkData, int jobs)
    throws InterruptedException, ExecutionException
    {
        return performScrub(cfs, skipCorrupted, checkData, false, jobs);
    }

    public AllSSTableOpStatus performScrub(final ColumnFamilyStore cfs, final boolean skipCorrupted, final boolean checkData, final boolean reinsertOverflowedTTLRows, int jobs)
    throws InterruptedException, ExecutionException
    {
        return parallelAllSSTableOperation(cfs, new OneSSTableOperation()
        {
            @Override
            public Iterable<SSTableReader> filterSSTables(LifecycleTransaction input)
            {
                return input.originals();
            }

            @Override
            public void execute(LifecycleTransaction input) throws IOException
            {
                scrubOne(cfs, input, skipCorrupted, checkData, reinsertOverflowedTTLRows);
            }
        }, jobs, OperationType.SCRUB);
    }

    public AllSSTableOpStatus performVerify(final ColumnFamilyStore cfs, final boolean extendedVerify) throws InterruptedException, ExecutionException
    {
        assert !cfs.isIndex();
        return parallelAllSSTableOperation(cfs, new OneSSTableOperation()
        {
            @Override
            public Iterable<SSTableReader> filterSSTables(LifecycleTransaction input)
            {
                return input.originals();
            }

            @Override
            public void execute(LifecycleTransaction input) throws IOException
            {
                verifyOne(cfs, input.onlyOne(), extendedVerify);
            }
        }, 0, OperationType.VERIFY);
    }

    public AllSSTableOpStatus performSSTableRewrite(final ColumnFamilyStore cfs, final boolean excludeCurrentVersion, int jobs) throws InterruptedException, ExecutionException
    {
        return parallelAllSSTableOperation(cfs, new OneSSTableOperation()
        {
            @Override
            public Iterable<SSTableReader> filterSSTables(LifecycleTransaction transaction)
            {
                Iterable<SSTableReader> sstables = new ArrayList<>(transaction.originals());
                Iterator<SSTableReader> iter = sstables.iterator();
                while (iter.hasNext())
                {
                    SSTableReader sstable = iter.next();
                    if (excludeCurrentVersion && sstable.descriptor.version.equals(sstable.descriptor.getFormat().getLatestVersion()))
                    {
                        transaction.cancel(sstable);
                        iter.remove();
                    }
                }
                return sstables;
            }

            @Override
            public void execute(LifecycleTransaction txn) throws IOException
            {
                AbstractCompactionTask task = cfs.getCompactionStrategy().getCompactionTask(txn, NO_GC, Long.MAX_VALUE);
                task.setUserDefined(true);
                task.setCompactionType(OperationType.UPGRADE_SSTABLES);
                task.execute(metrics);
            }
        }, jobs, OperationType.UPGRADE_SSTABLES);
    }

    public AllSSTableOpStatus performCleanup(final ColumnFamilyStore cfStore, int jobs) throws InterruptedException, ExecutionException
    {
        assert !cfStore.isIndex();
        Keyspace keyspace = cfStore.keyspace;
        if (!StorageService.instance.isJoined())
        {
            logger.info("Cleanup cannot run before a node has joined the ring");
            return AllSSTableOpStatus.ABORTED;
        }
        final Collection<Range<Token>> ranges = StorageService.instance.getLocalRanges(keyspace.getName());
        if (ranges.isEmpty())
        {
            logger.info("Node owns no data for keyspace {}", keyspace.getName());
            return AllSSTableOpStatus.SUCCESSFUL;
        }
        final boolean hasIndexes = cfStore.indexManager.hasIndexes();

        return parallelAllSSTableOperation(cfStore, new OneSSTableOperation()
        {
            @Override
            public Iterable<SSTableReader> filterSSTables(LifecycleTransaction transaction)
            {
                List<SSTableReader> sortedSSTables = Lists.newArrayList(transaction.originals());
                Collections.sort(sortedSSTables, new SSTableReader.SizeComparator());
                return sortedSSTables;
            }

            @Override
            public void execute(LifecycleTransaction txn) throws IOException
            {
                CleanupStrategy cleanupStrategy = CleanupStrategy.get(cfStore, ranges);
                doCleanupOne(cfStore, txn, cleanupStrategy, ranges, hasIndexes);
            }
        }, jobs, OperationType.CLEANUP);
    }

    public boolean checkIfFullyClean(final ColumnFamilyStore cfStore, int jobs) throws InterruptedException, ExecutionException
    {
        assert !cfStore.isIndex();
        Keyspace keyspace = cfStore.keyspace;
        if (!StorageService.instance.isJoined())
        {
            logger.info("Cleanup cannot run before a node has joined the ring");
            return false;
        }
        final Collection<Range<Token>> ranges = StorageService.instance.getLocalRanges(keyspace.getName());
        if (ranges.isEmpty())
        {
            logger.info("Node owns no data for keyspace {}", keyspace.getName());
            return true;
        }
        final boolean hasIndexes = cfStore.indexManager.hasIndexes();

        Optional<List<Boolean>> result = parallelAllSSTableCallable(cfStore, new OneSSTableCallable<Boolean>()
        {
            @Override
            public Iterable<SSTableReader> filterSSTables(LifecycleTransaction transaction)
            {
                List<SSTableReader> sortedSSTables = Lists.newArrayList(transaction.originals());
                Collections.sort(sortedSSTables, new SSTableReader.SizeComparator());
                return sortedSSTables;
            }

            @Override
            public Boolean execute(LifecycleTransaction txn) throws IOException
            {
                return checksIfCleanupNeededOne(txn, ranges, hasIndexes);
            }
        }, jobs, OperationType.CLEANUP);

        if (!result.isPresent())
        {
            logger.warn("Could not determine if cleanup is required.");
            return false;
        }

        // If even one sstable requires cleanup, then we consider this entire cf to require cleanup
        return !result.get().contains(true);
    }

    /**
     * Submit anti-compactions for a collection of SSTables over a set of repaired ranges and marks corresponding SSTables
     * as repaired.
     *
     * @param cfs Column family for anti-compaction
     * @param ranges Repaired ranges to be anti-compacted into separate SSTables.
     * @param sstables {@link Refs} of SSTables within CF to anti-compact.
     * @param repairedAt Unix timestamp of when repair was completed.
     * @return Futures executing anti-compaction.
     */
    public ListenableFuture<?> submitAntiCompaction(final ColumnFamilyStore cfs,
                                          final Collection<Range<Token>> ranges,
                                          final Refs<SSTableReader> sstables,
                                          final long repairedAt)
    {
        Runnable runnable = new WrappedRunnable() {
            @Override
            @SuppressWarnings("resource")
            public void runMayThrow() throws Exception
            {
                LifecycleTransaction modifier = null;
                while (modifier == null)
                {
                    for (SSTableReader compactingSSTable : cfs.getTracker().getCompacting())
                        sstables.releaseIfHolds(compactingSSTable);
                    // We don't anti-compact any SSTable that has been compacted during repair as it may have been compacted
                    // with unrepaired data.
                    Set<SSTableReader> compactedSSTables = new HashSet<>();
                    for (SSTableReader sstable : sstables)
                        if (sstable.isMarkedCompacted())
                            compactedSSTables.add(sstable);
                    sstables.release(compactedSSTables);
                    modifier = cfs.getTracker().tryModify(sstables, OperationType.ANTICOMPACTION);
                }
                performAnticompaction(cfs, ranges, sstables, modifier, repairedAt);
            }
        };

        ListenableFuture<?> ret = null;
        try
        {
            ret = executor.submitIfRunning(runnable, "anticompaction");
            return ret;
        }
        finally
        {
            if (ret == null || ret.isCancelled())
                sstables.release();
        }
    }

    /**
     * Make sure the {validatedForRepair} are marked for compaction before calling this.
     *
     * Caller must reference the validatedForRepair sstables (via ParentRepairSession.getActiveRepairedSSTableRefs(..)).
     *
     * NOTE: Repairs can take place on both unrepaired (incremental + full) and repaired (full) data.
     * Although anti-compaction could work on repaired sstables as well and would result in having more accurate
     * repairedAt values for these, we avoid anti-compacting already repaired sstables, as we currently don't
     * make use of any actual repairedAt value and splitting up sstables just for that is not worth it. However, we will
     * still update repairedAt if the SSTable is fully contained within the repaired ranges, as this does not require
     * anticompaction.
     *
     * @param cfs
     * @param ranges Ranges that the repair was carried out on
     * @param validatedForRepair SSTables containing the repaired ranges. Should be referenced before passing them.
     * @param txn Transaction across all SSTables that were repaired.
     * @throws InterruptedException
     * @throws IOException
     */
    public void performAnticompaction(ColumnFamilyStore cfs,
                                      Collection<Range<Token>> ranges,
                                      Refs<SSTableReader> validatedForRepair,
                                      LifecycleTransaction txn,
                                      long repairedAt) throws InterruptedException, IOException
    {
        logger.info("Starting anticompaction for {}.{} on {}/{} sstables", cfs.keyspace.getName(), cfs.getColumnFamilyName(), validatedForRepair.size(), cfs.getSSTables().size());
        logger.trace("Starting anticompaction for ranges {}", ranges);
        Set<SSTableReader> sstables = new HashSet<>(validatedForRepair);
        Set<SSTableReader> mutatedRepairStatuses = new HashSet<>(); // SSTables that were completely repaired only
        Set<SSTableReader> nonAnticompacting = new HashSet<>();

        Iterator<SSTableReader> sstableIterator = sstables.iterator();
        try
        {
            List<Range<Token>> normalizedRanges = Range.normalize(ranges);

            while (sstableIterator.hasNext())
            {
                SSTableReader sstable = sstableIterator.next();
                List<String> anticompactRanges = new ArrayList<>();
                // We don't anti-compact SSTables already marked repaired. See CASSANDRA-13153
                // and CASSANDRA-14423.
                if (sstable.isRepaired()) // We never anti-compact already repaired SSTables
                    nonAnticompacting.add(sstable);

                Bounds<Token> sstableBounds = new Bounds<>(sstable.first.getToken(), sstable.last.getToken());

                boolean shouldAnticompact = false;

                for (Range<Token> r : normalizedRanges)
                {
                    if (r.contains(sstableBounds.left) && r.contains(sstableBounds.right))
                    {
                        logger.info("SSTable {} fully contained in range {}, mutating repairedAt instead of anticompacting", sstable, r);
                        sstable.descriptor.getMetadataSerializer().mutateRepairedAt(sstable.descriptor, repairedAt);
                        sstable.reloadSSTableMetadata();
                        if (!nonAnticompacting.contains(sstable)) // don't notify if the SSTable was already repaired
                            mutatedRepairStatuses.add(sstable);
                        sstableIterator.remove();
                        shouldAnticompact = true;
                        break;
                    }
                    else if (r.intersects(sstableBounds) && !nonAnticompacting.contains(sstable))
                    {
                        anticompactRanges.add(r.toString());
                        shouldAnticompact = true;
                    }
                }

                if (!anticompactRanges.isEmpty())
                    logger.info("SSTable {} ({}) will be anticompacted on ranges: {}", sstable, sstableBounds, StringUtils.join(anticompactRanges, ", "));

                if (!shouldAnticompact)
                {
                    logger.info("SSTable {} ({}) not subject to anticompaction of repaired ranges {}, not touching repairedAt.", sstable, sstableBounds, normalizedRanges);
                    nonAnticompacting.add(sstable);
                    sstableIterator.remove();
                }
            }
            cfs.getTracker().notifySSTableRepairedStatusChanged(mutatedRepairStatuses);
            txn.cancel(Sets.union(nonAnticompacting, mutatedRepairStatuses));
            validatedForRepair.release(Sets.union(nonAnticompacting, mutatedRepairStatuses));
            assert txn.originals().equals(sstables);
            if (!sstables.isEmpty())
                doAntiCompaction(cfs, ranges, txn, repairedAt);
            txn.finish();
        }
        finally
        {
            validatedForRepair.release();
            txn.close();
        }

        logger.info("Completed anticompaction successfully");
    }

    public void performMaximal(final ColumnFamilyStore cfStore, boolean splitOutput)
    {
        FBUtilities.waitOnFutures(submitMaximal(cfStore, getDefaultGcBefore(cfStore), splitOutput));
    }

    public List<Future<?>> submitMaximal(final ColumnFamilyStore cfStore, final int gcBefore, boolean splitOutput)
    {
        // here we compute the task off the compaction executor, so having that present doesn't
        // confuse runWithCompactionsDisabled -- i.e., we don't want to deadlock ourselves, waiting
        // for ourselves to finish/acknowledge cancellation before continuing.
        final Collection<AbstractCompactionTask> tasks = cfStore.getCompactionStrategy().getMaximalTask(gcBefore, splitOutput);

        if (tasks == null)
            return Collections.emptyList();

        List<Future<?>> futures = new ArrayList<>();
        int nonEmptyTasks = 0;
        for (final AbstractCompactionTask task : tasks)
        {
            if (task.transaction.originals().size() > 0)
                nonEmptyTasks++;
            Runnable runnable = new WrappedRunnable()
            {
                protected void runMayThrow() throws IOException
                {
                    task.execute(metrics);
                }
            };

            Future<?> fut = executor.submitIfRunning(runnable, "maximal task");
            if (!fut.isCancelled())
                futures.add(fut);
        }
        if (nonEmptyTasks > 1)
            logger.info("Cannot perform a full major compaction as repaired and unrepaired sstables cannot be compacted together. These two set of sstables will be compacted separately.");
        return futures;
    }

    public void forceUserDefinedCompaction(String dataFiles)
    {
        String[] filenames = dataFiles.split(",");
        Multimap<ColumnFamilyStore, Descriptor> descriptors = ArrayListMultimap.create();

        for (String filename : filenames)
        {
            // extract keyspace and columnfamily name from filename
            Descriptor desc = Descriptor.fromFilename(filename.trim());
            if (Schema.instance.getCFMetaData(desc) == null)
            {
                logger.warn("Schema does not exist for file {}. Skipping.", filename);
                continue;
            }
            // group by keyspace/columnfamily
            ColumnFamilyStore cfs = Keyspace.open(desc.ksname).getColumnFamilyStore(desc.cfname);
            descriptors.put(cfs, cfs.directories.find(new File(filename.trim()).getName()));
        }

        List<Future<?>> futures = new ArrayList<>();
        for (ColumnFamilyStore cfs : descriptors.keySet())
            futures.add(submitUserDefined(cfs, descriptors.get(cfs), getDefaultGcBefore(cfs)));
        FBUtilities.waitOnFutures(futures);
    }

    public Future<?> submitUserDefined(final ColumnFamilyStore cfs, final Collection<Descriptor> dataFiles, final int gcBefore)
    {
        Runnable runnable = new WrappedRunnable()
        {
            protected void runMayThrow() throws IOException
            {
                // look up the sstables now that we're on the compaction executor, so we don't try to re-compact
                // something that was already being compacted earlier.
                Collection<SSTableReader> sstables = new ArrayList<>(dataFiles.size());
                for (Descriptor desc : dataFiles)
                {
                    // inefficient but not in a performance sensitive path
                    SSTableReader sstable = lookupSSTable(cfs, desc);
                    if (sstable == null)
                    {
                        logger.info("Will not compact {}: it is not an active sstable", desc);
                    }
                    else
                    {
                        sstables.add(sstable);
                    }
                }

                if (sstables.isEmpty())
                {
                    logger.info("No files to compact for user defined compaction");
                }
                else
                {
                    AbstractCompactionTask task = cfs.getCompactionStrategy().getUserDefinedTask(sstables, gcBefore);
                    if (task != null)
                        task.execute(metrics);
                }
            }
        };

        return executor.submitIfRunning(runnable, "user defined task");
    }

    // This acquire a reference on the sstable
    // This is not efficient, do not use in any critical path
    private SSTableReader lookupSSTable(final ColumnFamilyStore cfs, Descriptor descriptor)
    {
        for (SSTableReader sstable : cfs.getSSTables())
        {
            if (sstable.descriptor.equals(descriptor))
                return sstable;
        }
        return null;
    }

    /**
     * Does not mutate data, so is not scheduled.
     */
    public Future<?> submitValidation(final ColumnFamilyStore cfStore, final Validator validator)
    {
        Callable<Object> callable = new Callable<Object>()
        {
            public Object call() throws IOException
            {
                try
                {
                    doValidationCompaction(cfStore, validator);
                }
                catch (Throwable e)
                {
                    // we need to inform the remote end of our failure, otherwise it will hang on repair forever
                    validator.fail();
                    throw e;
                }
                return this;
            }
        };

        return validationExecutor.submitIfRunning(callable, "validation");
    }

    /* Used in tests. */
    public void disableAutoCompaction()
    {
        for (String ksname : Schema.instance.getNonSystemKeyspaces())
        {
            for (ColumnFamilyStore cfs : Keyspace.open(ksname).getColumnFamilyStores())
                cfs.disableAutoCompaction();
        }
    }

    private void scrubOne(ColumnFamilyStore cfs, LifecycleTransaction modifier, boolean skipCorrupted, boolean checkData, boolean reinsertOverflowedTTLRows) throws IOException
    {
        CompactionInfo.Holder scrubInfo = null;

        try (Scrubber scrubber = new Scrubber(cfs, modifier, skipCorrupted, checkData, reinsertOverflowedTTLRows))
        {
            scrubInfo = scrubber.getScrubInfo();
            metrics.beginCompaction(scrubInfo);
            scrubber.scrub();
        }
        finally
        {
            if (scrubInfo != null)
                metrics.finishCompaction(scrubInfo);
        }
    }

    private void verifyOne(ColumnFamilyStore cfs, SSTableReader sstable, boolean extendedVerify) throws IOException
    {
        CompactionInfo.Holder verifyInfo = null;

        try (Verifier verifier = new Verifier(cfs, sstable, false))
        {
            verifyInfo = verifier.getVerifyInfo();
            metrics.beginCompaction(verifyInfo);
            verifier.verify(extendedVerify);
        }
        finally
        {
            if (verifyInfo != null)
                metrics.finishCompaction(verifyInfo);
        }
    }

    /**
     * Determines if a cleanup would actually remove any data in this SSTable based
     * on a set of owned ranges.
     */
    @VisibleForTesting
    public static boolean needsCleanup(SSTableReader sstable, Collection<Range<Token>> ownedRanges)
    {
        assert !ownedRanges.isEmpty(); // cleanup checks for this

        // unwrap and sort the ranges by LHS token
        List<Range<Token>> sortedRanges = Range.normalize(ownedRanges);

        // see if there are any keys LTE the token for the start of the first range
        // (token range ownership is exclusive on the LHS.)
        Range<Token> firstRange = sortedRanges.get(0);
        if (sstable.first.getToken().compareTo(firstRange.left) <= 0)
            return true;

        // then, iterate over all owned ranges and see if the next key beyond the end of the owned
        // range falls before the start of the next range
        for (int i = 0; i < sortedRanges.size(); i++)
        {
            Range<Token> range = sortedRanges.get(i);
            if (range.right.isMinimum())
            {
                // we split a wrapping range and this is the second half.
                // there can't be any keys beyond this (and this is the last range)
                return false;
            }

            DecoratedKey firstBeyondRange = sstable.firstKeyBeyond(range.right.maxKeyBound());
            if (firstBeyondRange == null)
            {
                // we ran off the end of the sstable looking for the next key; we don't need to check any more ranges
                return false;
            }

            if (i == (sortedRanges.size() - 1))
            {
                // we're at the last range and we found a key beyond the end of the range
                return true;
            }

            Range<Token> nextRange = sortedRanges.get(i + 1);
            if (firstBeyondRange.getToken().compareTo(nextRange.left) <= 0)
            {
                // we found a key in between the owned ranges
                return true;
            }
        }

        return false;
    }

    private boolean checksIfCleanupNeededOne(LifecycleTransaction txn, Collection<Range<Token>> ranges, boolean hasIndexes)
    {
        SSTableReader sstable = txn.onlyOne();
        if (!hasIndexes && !new Bounds<>(sstable.first.getToken(), sstable.last.getToken()).intersects(ranges))
        {
            txn.obsoleteOriginals();
            txn.finish();
            return false;
        }
        if (!needsCleanup(sstable, ranges))
        {
            logger.trace("Skipping {} for cleanup; all rows should be kept", sstable);
            return false;
        }
        return true;
    }

    /**
     * This function goes over a file and removes the keys that the node is not responsible for
     * and only keeps keys that this node is responsible for.
     *
     * @throws IOException
     */
    @SuppressWarnings("resource")
    private void doCleanupOne(final ColumnFamilyStore cfs, LifecycleTransaction txn, CleanupStrategy cleanupStrategy, Collection<Range<Token>> ranges, boolean hasIndexes) throws IOException
    {
        assert !cfs.isIndex();

        if (!checksIfCleanupNeededOne(txn, ranges, hasIndexes)) {
            return;
        }
        SSTableReader sstable = txn.onlyOne();

        long start = System.nanoTime();

        long totalkeysWritten = 0;

        long expectedBloomFilterSize = Math.max(cfs.metadata.getMinIndexInterval(),
                                               SSTableReader.getApproximateKeyCount(txn.originals()));
        if (logger.isTraceEnabled())
            logger.trace("Expected bloom filter size : {}", expectedBloomFilterSize);

        logger.info("Cleaning up {}", sstable);

        File compactionFileLocation = cfs.directories.getWriteableLocationAsFile(cfs.getExpectedCompactedFileSize(txn.originals(), OperationType.CLEANUP));
        if (compactionFileLocation == null)
            throw new IOException("disk full");

        ISSTableScanner scanner = cleanupStrategy.getScanner(sstable, getRateLimiter());
        CleanupInfo ci = new CleanupInfo(sstable, scanner);

        metrics.beginCompaction(ci);
        List<SSTableReader> finished;
        try (SSTableRewriter writer = new SSTableRewriter(cfs, txn, sstable.maxDataAge, false);
             CompactionController controller = new CompactionController(cfs, txn.originals(), getDefaultGcBefore(cfs));
             Refs<SSTableReader> refs = Refs.ref(Collections.singleton(sstable)))
        {
            writer.switchWriter(createWriter(cfs, compactionFileLocation, expectedBloomFilterSize, sstable.getSSTableMetadata().repairedAt, sstable));

            while (scanner.hasNext())
            {
                if (ci.isStopRequested())
                    throw new CompactionInterruptedException(ci.getCompactionInfo());

                @SuppressWarnings("resource")
                SSTableIdentityIterator row = cleanupStrategy.cleanup((SSTableIdentityIterator) scanner.next());
                if (row == null)
                    continue;
                @SuppressWarnings("resource")
                AbstractCompactedRow compactedRow = new LazilyCompactedRow(controller, Collections.singletonList(row));
                if (writer.append(compactedRow) != null)
                    totalkeysWritten++;
            }

            // flush to ensure we don't lose the tombstones on a restart, since they are not commitlog'd
            cfs.indexManager.flushIndexesBlocking("Cleanup");

            finished = writer.finish();
        }
        finally
        {
            scanner.close();
            metrics.finishCompaction(ci);
        }

        if (!finished.isEmpty())
        {
            String format = "Cleaned up to %s.  %,d to %,d (~%d%% of original) bytes for %,d keys.  Time: %,dms.";
            long dTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            long startsize = sstable.onDiskLength();
            long endsize = 0;
            for (SSTableReader newSstable : finished)
                endsize += newSstable.onDiskLength();
            double ratio = (double) endsize / (double) startsize;
            logger.info(String.format(format, finished.get(0).getFilename(), startsize, endsize, (int) (ratio * 100), totalkeysWritten, dTime));
        }

    }

    private static abstract class CleanupStrategy
    {
        public static CleanupStrategy get(ColumnFamilyStore cfs, Collection<Range<Token>> ranges)
        {
            return cfs.indexManager.hasIndexes()
                 ? new Full(cfs, ranges)
                 : new Bounded(cfs, ranges);
        }

        public abstract ISSTableScanner getScanner(SSTableReader sstable, RateLimiter limiter);
        public abstract SSTableIdentityIterator cleanup(SSTableIdentityIterator row);

        private static final class Bounded extends CleanupStrategy
        {
            private final Collection<Range<Token>> ranges;

            public Bounded(final ColumnFamilyStore cfs, Collection<Range<Token>> ranges)
            {
                this.ranges = ranges;
                instance.cacheCleanupExecutor.submit(new Runnable()
                {
                    @Override
                    public void run()
                    {
                        cfs.cleanupCache();
                    }
                });

            }
            @Override
            public ISSTableScanner getScanner(SSTableReader sstable, RateLimiter limiter)
            {
                return sstable.getScanner(ranges, limiter);
            }

            @Override
            public SSTableIdentityIterator cleanup(SSTableIdentityIterator row)
            {
                return row;
            }
        }

        private static final class Full extends CleanupStrategy
        {
            private final Collection<Range<Token>> ranges;
            private final ColumnFamilyStore cfs;
            private List<Cell> indexedColumnsInRow;

            public Full(ColumnFamilyStore cfs, Collection<Range<Token>> ranges)
            {
                this.cfs = cfs;
                this.ranges = ranges;
                this.indexedColumnsInRow = null;
            }

            @Override
            public ISSTableScanner getScanner(SSTableReader sstable, RateLimiter limiter)
            {
                return sstable.getScanner(limiter);
            }

            @Override
            public SSTableIdentityIterator cleanup(SSTableIdentityIterator row)
            {
                if (Range.isInRanges(row.getKey().getToken(), ranges))
                    return row;

                cfs.invalidateCachedRow(row.getKey());

                if (indexedColumnsInRow != null)
                    indexedColumnsInRow.clear();

                while (row.hasNext())
                {
                    OnDiskAtom column = row.next();

                    if (column instanceof Cell && cfs.indexManager.indexes((Cell) column))
                    {
                        if (indexedColumnsInRow == null)
                            indexedColumnsInRow = new ArrayList<>();

                        indexedColumnsInRow.add((Cell) column);
                    }
                }

                if (indexedColumnsInRow != null && !indexedColumnsInRow.isEmpty())
                {
                    // acquire memtable lock here because secondary index deletion may cause a race. See CASSANDRA-3712
                    try (OpOrder.Group opGroup = cfs.keyspace.writeOrder.start())
                    {
                        cfs.indexManager.deleteFromIndexes(row.getKey(), indexedColumnsInRow, opGroup);
                    }
                }
                return null;
            }
        }
    }

    public static SSTableWriter createWriter(ColumnFamilyStore cfs,
                                             File compactionFileLocation,
                                             long expectedBloomFilterSize,
                                             long repairedAt,
                                             SSTableReader sstable)
    {
        FileUtils.createDirectory(compactionFileLocation);

        return SSTableWriter.create(cfs.metadata,
                                    Descriptor.fromFilename(cfs.getTempSSTablePath(compactionFileLocation)),
                                    expectedBloomFilterSize,
                                    repairedAt,
                                    sstable.getSSTableLevel(),
                                    cfs.partitioner);
    }

    public static SSTableWriter createWriterForAntiCompaction(ColumnFamilyStore cfs,
                                             File compactionFileLocation,
                                             int expectedBloomFilterSize,
                                             long repairedAt,
                                             Collection<SSTableReader> sstables)
    {
        FileUtils.createDirectory(compactionFileLocation);
        int minLevel = Integer.MAX_VALUE;
        // if all sstables have the same level, we can compact them together without creating overlap during anticompaction
        // note that we only anticompact from unrepaired sstables, which is not leveled, but we still keep original level
        // after first migration to be able to drop the sstables back in their original place in the repaired sstable manifest
        for (SSTableReader sstable : sstables)
        {
            if (minLevel == Integer.MAX_VALUE)
                minLevel = sstable.getSSTableLevel();

            if (minLevel != sstable.getSSTableLevel())
            {
                minLevel = 0;
                break;
            }
        }
        return SSTableWriter.create(Descriptor.fromFilename(cfs.getTempSSTablePath(compactionFileLocation)),
                                    (long) expectedBloomFilterSize,
                                    repairedAt,
                                    cfs.metadata,
                                    cfs.partitioner,
                                    new MetadataCollector(sstables, cfs.metadata.comparator, minLevel));
    }


    /**
     * Performs a readonly "compaction" of all sstables in order to validate complete rows,
     * but without writing the merge result
     */
    @SuppressWarnings("resource")
    private void doValidationCompaction(ColumnFamilyStore cfs, Validator validator) throws IOException
    {
        // this isn't meant to be race-proof, because it's not -- it won't cause bugs for a CFS to be dropped
        // mid-validation, or to attempt to validate a droped CFS.  this is just a best effort to avoid useless work,
        // particularly in the scenario where a validation is submitted before the drop, and there are compactions
        // started prior to the drop keeping some sstables alive.  Since validationCompaction can run
        // concurrently with other compactions, it would otherwise go ahead and scan those again.
        if (!cfs.isValid())
            return;

        Refs<SSTableReader> sstables = null;
        try
        {

            int gcBefore;
            UUID parentRepairSessionId = validator.desc.parentSessionId;
            String snapshotName;
            boolean isGlobalSnapshotValidation = cfs.snapshotExists(parentRepairSessionId.toString());
            if (isGlobalSnapshotValidation)
                snapshotName = parentRepairSessionId.toString();
            else
                snapshotName = validator.desc.sessionId.toString();
            boolean isSnapshotValidation = cfs.snapshotExists(snapshotName);

            if (isSnapshotValidation)
            {
                // If there is a snapshot created for the session then read from there.
                // note that we populate the parent repair session when creating the snapshot, meaning the sstables in the snapshot are the ones we
                // are supposed to validate.
                sstables = cfs.getSnapshotSSTableReader(snapshotName);


                // Computing gcbefore based on the current time wouldn't be very good because we know each replica will execute
                // this at a different time (that's the whole purpose of repair with snaphsot). So instead we take the creation
                // time of the snapshot, which should give us roughtly the same time on each replica (roughtly being in that case
                // 'as good as in the non-snapshot' case)
                gcBefore = cfs.gcBefore(cfs.getSnapshotCreationTime(snapshotName));
            }
            else
            {
                // flush first so everyone is validating data that is as similar as possible
                StorageService.instance.forceKeyspaceFlush(cfs.keyspace.getName(), cfs.name);
                sstables = getSSTablesToValidate(cfs, validator);
                if (sstables == null)
                    return; // this means the parent repair session was removed - the repair session failed on another node and we removed it
                if (validator.gcBefore > 0)
                    gcBefore = validator.gcBefore;
                else
                    gcBefore = getDefaultGcBefore(cfs);
            }

            // Create Merkle tree suitable to hold estimated partitions for given range.
            // We blindly assume that partition is evenly distributed on all sstables for now.
            long numPartitions = 0;
            for (SSTableReader sstable : sstables)
            {
                numPartitions += sstable.estimatedKeysForRanges(singleton(validator.desc.range));
            }
            // determine tree depth from number of partitions, but cap at 20 to prevent large tree (CASSANDRA-5263)
            int depth = numPartitions > 0 ? (int) Math.min(Math.ceil(Math.log(numPartitions) / Math.log(2)), 20) : 0;
            MerkleTree tree = new MerkleTree(cfs.partitioner, validator.desc.range, MerkleTree.RECOMMENDED_DEPTH, (int) Math.pow(2, depth));

            long start = System.nanoTime();
            try (AbstractCompactionStrategy.ScannerList scanners = cfs.getCompactionStrategy().getScanners(sstables, validator.desc.range))
            {
                CompactionIterable ci = new ValidationCompactionIterable(cfs, scanners.scanners, gcBefore);
                Iterator<AbstractCompactedRow> iter = ci.iterator();
                metrics.beginCompaction(ci);
                try
                {
                    // validate the CF as we iterate over it
                    validator.prepare(cfs, tree);
                    while (iter.hasNext())
                    {
                        if (ci.isStopRequested())
                            throw new CompactionInterruptedException(ci.getCompactionInfo());
                        AbstractCompactedRow row = iter.next();
                        validator.add(row);
                    }
                    validator.complete();
                }
                finally
                {
                    // we can only clear the snapshot if we are not doing a global snapshot validation (we then clear it once anticompaction
                    // is done).
                    if (isSnapshotValidation && !isGlobalSnapshotValidation)
                    {
                        cfs.clearSnapshot(snapshotName);
                    }

                    metrics.finishCompaction(ci);
                }
            }

            if (logger.isTraceEnabled())
            {
                // MT serialize may take time
                long duration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                logger.trace("Validation finished in {} msec, depth {} for {} keys, serialized size {} bytes for {}",
                             duration,
                             depth,
                             numPartitions,
                             MerkleTree.serializer.serializedSize(tree, 0),
                             validator.desc);
            }
        }
        finally
        {
            if (sstables != null)
                sstables.release();
        }
    }

    private synchronized Refs<SSTableReader> getSSTablesToValidate(ColumnFamilyStore cfs, Validator validator)
    {
        Refs<SSTableReader> sstables;

        ActiveRepairService.ParentRepairSession prs = ActiveRepairService.instance.getParentRepairSession(validator.desc.parentSessionId);
        if (prs == null)
            return null;
        Set<SSTableReader> sstablesToValidate = new HashSet<>();

        if (prs.isGlobal)
            prs.markSSTablesRepairing(cfs.metadata.cfId, validator.desc.parentSessionId);

        // note that we always grab all existing sstables for this - if we were to just grab the ones that
        // were marked as repairing, we would miss any ranges that were compacted away and this would cause us to overstream
        try (ColumnFamilyStore.RefViewFragment sstableCandidates = cfs.selectAndReference(prs.isIncremental ? ColumnFamilyStore.UNREPAIRED_SSTABLES : ColumnFamilyStore.CANONICAL_SSTABLES))
        {
            for (SSTableReader sstable : sstableCandidates.sstables)
            {
                if (new Bounds<>(sstable.first.getToken(), sstable.last.getToken()).intersects(Collections.singletonList(validator.desc.range)))
                {
                    sstablesToValidate.add(sstable);
                }
            }

            sstables = Refs.tryRef(sstablesToValidate);
            if (sstables == null)
            {
                logger.error("Could not reference sstables");
                throw new RuntimeException("Could not reference sstables");
            }
        }

        return sstables;
    }

    /**
     * Splits up an sstable into two new sstables. The first of the new tables will store repaired ranges, the second
     * will store the non-repaired ranges. Once anticompation is completed, the original sstable is marked as compacted
     * and subsequently deleted.
     * @param cfs
     * @param repaired a transaction over the repaired sstables to anticompacy
     * @param ranges Repaired ranges to be placed into one of the new sstables. The repaired table will be tracked via
     * the {@link org.apache.cassandra.io.sstable.metadata.StatsMetadata#repairedAt} field.
     */
    private void doAntiCompaction(ColumnFamilyStore cfs, Collection<Range<Token>> ranges, LifecycleTransaction repaired, long repairedAt)
    {
        int numAnticompact = repaired.originals().size();
        logger.info("Performing anticompaction on {} sstables", numAnticompact);

        //Group SSTables
        Collection<Collection<SSTableReader>> groupedSSTables = cfs.getCompactionStrategy().groupSSTablesForAntiCompaction(repaired.originals());
        // iterate over sstables to check if the repaired / unrepaired ranges intersect them.
        int antiCompactedSSTableCount = 0;
        for (Collection<SSTableReader> sstableGroup : groupedSSTables)
        {
            try (LifecycleTransaction txn = repaired.split(sstableGroup))
            {
                int antiCompacted = antiCompactGroup(cfs, ranges, txn, repairedAt);
                antiCompactedSSTableCount += antiCompacted;
            }
        }

        String format = "Anticompaction completed successfully, anticompacted from {} to {} sstable(s).";
        logger.info(format, numAnticompact, antiCompactedSSTableCount);
    }

    private int antiCompactGroup(ColumnFamilyStore cfs, Collection<Range<Token>> ranges,
                             LifecycleTransaction anticompactionGroup, long repairedAt)
    {
        long groupMaxDataAge = -1;

        for (Iterator<SSTableReader> i = anticompactionGroup.originals().iterator(); i.hasNext();)
        {
            SSTableReader sstable = i.next();
            if (groupMaxDataAge < sstable.maxDataAge)
                groupMaxDataAge = sstable.maxDataAge;
        }

        if (anticompactionGroup.originals().size() == 0)
        {
            logger.info("No valid anticompactions for this group, All sstables were compacted and are no longer available");
            return 0;
        }

        logger.info("Anticompacting {}", anticompactionGroup);
        Set<SSTableReader> sstableAsSet = anticompactionGroup.originals();

        File destination = cfs.directories.getWriteableLocationAsFile(cfs.getExpectedCompactedFileSize(sstableAsSet, OperationType.ANTICOMPACTION));
        long repairedKeyCount = 0;
        long unrepairedKeyCount = 0;
        AbstractCompactionStrategy strategy = cfs.getCompactionStrategy();
        try (SSTableRewriter repairedSSTableWriter = new SSTableRewriter(cfs, anticompactionGroup, groupMaxDataAge, false, false);
             SSTableRewriter unRepairedSSTableWriter = new SSTableRewriter(cfs, anticompactionGroup, groupMaxDataAge, false, false);
             AbstractCompactionStrategy.ScannerList scanners = strategy.getScanners(anticompactionGroup.originals());
             CompactionController controller = new CompactionController(cfs, sstableAsSet, getDefaultGcBefore(cfs)))
        {
            int expectedBloomFilterSize = Math.max(cfs.metadata.getMinIndexInterval(), (int)(SSTableReader.getApproximateKeyCount(sstableAsSet)));

            repairedSSTableWriter.switchWriter(CompactionManager.createWriterForAntiCompaction(cfs, destination, expectedBloomFilterSize, repairedAt, sstableAsSet));
            unRepairedSSTableWriter.switchWriter(CompactionManager.createWriterForAntiCompaction(cfs, destination, expectedBloomFilterSize, ActiveRepairService.UNREPAIRED_SSTABLE, sstableAsSet));

            CompactionIterable ci = new CompactionIterable(OperationType.ANTICOMPACTION, scanners.scanners, controller, DatabaseDescriptor.getSSTableFormat(), UUIDGen.getTimeUUID());
            metrics.beginCompaction(ci);
            try
            {
                @SuppressWarnings("resource")
                CloseableIterator<AbstractCompactedRow> iter = ci.iterator();
                Range.OrderedRangeContainmentChecker containmentChecker = new Range.OrderedRangeContainmentChecker(ranges);
                while (iter.hasNext())
                {
                    @SuppressWarnings("resource")
                    AbstractCompactedRow row = iter.next();
                    // if current range from sstable is repaired, save it into the new repaired sstable
                    if (containmentChecker.contains(row.key.getToken()))
                    {
                        repairedSSTableWriter.append(row);
                        repairedKeyCount++;
                    }
                    // otherwise save into the new 'non-repaired' table
                    else
                    {
                        unRepairedSSTableWriter.append(row);
                        unrepairedKeyCount++;
                    }
                }
            }
            finally
            {
                metrics.finishCompaction(ci);
            }

            List<SSTableReader> anticompactedSSTables = new ArrayList<>();
            // since both writers are operating over the same Transaction, we cannot use the convenience Transactional.finish() method,
            // as on the second finish() we would prepareToCommit() on a Transaction that has already been committed, which is forbidden by the API
            // (since it indicates misuse). We call permitRedundantTransitions so that calls that transition to a state already occupied are permitted.
            anticompactionGroup.permitRedundantTransitions();
            repairedSSTableWriter.setRepairedAt(repairedAt).prepareToCommit();
            unRepairedSSTableWriter.prepareToCommit();
            anticompactedSSTables.addAll(repairedSSTableWriter.finished());
            anticompactedSSTables.addAll(unRepairedSSTableWriter.finished());
            repairedSSTableWriter.commit();
            unRepairedSSTableWriter.commit();

            logger.trace("Repaired {} keys out of {} for {}/{} in {}", repairedKeyCount,
                                                                       repairedKeyCount + unrepairedKeyCount,
                                                                       cfs.keyspace.getName(),
                                                                       cfs.getColumnFamilyName(),
                                                                       anticompactionGroup);
            return anticompactedSSTables.size();
        }
        catch (Throwable e)
        {
            JVMStabilityInspector.inspectThrowable(e);
            logger.error("Error anticompacting " + anticompactionGroup, e);
        }
        return 0;
    }

    /**
     * Is not scheduled, because it is performing disjoint work from sstable compaction.
     */
    public Future<?> submitIndexBuild(final SecondaryIndexBuilder builder)
    {
        Runnable runnable = new Runnable()
        {
            public void run()
            {
                metrics.beginCompaction(builder);
                try
                {
                    builder.build();
                }
                finally
                {
                    metrics.finishCompaction(builder);
                }
            }
        };

        return executor.submitIfRunning(runnable, "index build");
    }

    public Future<?> submitCacheWrite(final AutoSavingCache.Writer writer)
    {
        Runnable runnable = new Runnable()
        {
            public void run()
            {
                if (!AutoSavingCache.flushInProgress.add(writer.cacheType()))
                {
                    logger.trace("Cache flushing was already in progress: skipping {}", writer.getCompactionInfo());
                    return;
                }
                try
                {
                    metrics.beginCompaction(writer);
                    try
                    {
                        writer.saveCache();
                    }
                    finally
                    {
                        metrics.finishCompaction(writer);
                    }
                }
                finally
                {
                    AutoSavingCache.flushInProgress.remove(writer.cacheType());
                }
            }
        };

        return executor.submitIfRunning(runnable, "cache write");
    }

    public List<SSTableReader> runIndexSummaryRedistribution(IndexSummaryRedistribution redistribution) throws IOException
    {
        metrics.beginCompaction(redistribution);

        try
        {
            return redistribution.redistributeSummaries();
        }
        finally
        {
            metrics.finishCompaction(redistribution);
        }
    }

    public static int getDefaultGcBefore(ColumnFamilyStore cfs)
    {
        // 2ndary indexes have ExpiringColumns too, so we need to purge tombstones deleted before now. We do not need to
        // add any GcGrace however since 2ndary indexes are local to a node.
        return cfs.isIndex() ? (int) (System.currentTimeMillis() / 1000) : cfs.gcBefore(System.currentTimeMillis());
    }

    private static class ValidationCompactionIterable extends CompactionIterable
    {
        @SuppressWarnings("resource")
        public ValidationCompactionIterable(ColumnFamilyStore cfs, List<ISSTableScanner> scanners, int gcBefore)
        {
            super(OperationType.VALIDATION, scanners, new ValidationCompactionController(cfs, gcBefore), DatabaseDescriptor.getSSTableFormat(), UUIDGen.getTimeUUID());
        }
    }

    /*
     * Controller for validation compaction that always purges.
     * Note that we should not call cfs.getOverlappingSSTables on the provided
     * sstables because those sstables are not guaranteed to be active sstables
     * (since we can run repair on a snapshot).
     */
    private static class ValidationCompactionController extends CompactionController
    {
        public ValidationCompactionController(ColumnFamilyStore cfs, int gcBefore)
        {
            super(cfs, gcBefore);
        }

        @Override
        public Predicate<Long> getPurgeEvaluator(DecoratedKey key)
        {
            /*
             * The main reason we always purge is that including gcable tombstone would mean that the
             * repair digest will depends on the scheduling of compaction on the different nodes. This
             * is still not perfect because gcbefore is currently dependend on the current time at which
             * the validation compaction start, which while not too bad for normal repair is broken for
             * repair on snapshots. A better solution would be to agree on a gcbefore that all node would
             * use, and we'll do that with CASSANDRA-4932.
             * Note validation compaction includes all sstables, so we don't have the problem of purging
             * a tombstone that could shadow a column in another sstable, but this is doubly not a concern
             * since validation compaction is read-only.
             */
            return Predicates.alwaysTrue();
        }
    }

    public int getActiveCompactions()
    {
        return CompactionMetrics.getCompactions().size();
    }

    static class CompactionExecutor extends JMXEnabledThreadPoolExecutor
    {
        protected CompactionExecutor(int minThreads, int maxThreads, String name, BlockingQueue<Runnable> queue)
        {
            super(minThreads, maxThreads, 60, TimeUnit.SECONDS, queue, new NamedThreadFactory(name, Thread.MIN_PRIORITY), "internal");
        }

        private CompactionExecutor(int threadCount, String name)
        {
            this(threadCount, threadCount, name, new LinkedBlockingQueue<Runnable>());
        }

        public CompactionExecutor()
        {
            this(Math.max(1, DatabaseDescriptor.getConcurrentCompactors()), "CompactionExecutor");
        }

        protected void beforeExecute(Thread t, Runnable r)
        {
            // can't set this in Thread factory, so we do it redundantly here
            isCompactionManager.set(true);
            super.beforeExecute(t, r);
        }

        // modified from DebuggableThreadPoolExecutor so that CompactionInterruptedExceptions are not logged
        @Override
        public void afterExecute(Runnable r, Throwable t)
        {
            DebuggableThreadPoolExecutor.maybeResetTraceSessionWrapper(r);

            if (t == null)
                t = DebuggableThreadPoolExecutor.extractThrowable(r);

            if (t != null)
            {
                if (t instanceof CompactionInterruptedException)
                {
                    logger.info(t.getMessage());
                    if (t.getSuppressed() != null && t.getSuppressed().length > 0)
                        logger.warn("Interruption of compaction encountered exceptions:", t);
                    else
                        logger.trace("Full interruption stack trace:", t);
                }
                else
                {
                    DebuggableThreadPoolExecutor.handleOrLog(t);
                }
            }

            // Snapshots cannot be deleted on Windows while segments of the root element are mapped in NTFS. Compactions
            // unmap those segments which could free up a snapshot for successful deletion.
            SnapshotDeletingTask.rescheduleFailedTasks();
        }

        public ListenableFuture<?> submitIfRunning(Runnable task, String name)
        {
            return submitIfRunning(Executors.callable(task, null), name);
        }

        /**
         * Submit the task but only if the executor has not been shutdown.If the executor has
         * been shutdown, or in case of a rejected execution exception return a cancelled future.
         *
         * @param task - the task to submit
         * @param name - the task name to use in log messages
         *
         * @return the future that will deliver the task result, or a future that has already been
         *         cancelled if the task could not be submitted.
         */
        public ListenableFuture<?> submitIfRunning(Callable<?> task, String name)
        {
            if (isShutdown())
            {
                logger.info("Executor has been shut down, not submitting {}", name);
                return Futures.immediateCancelledFuture();
            }

            try
            {
                ListenableFutureTask ret = ListenableFutureTask.create(task);
                execute(ret);
                return ret;
            }
            catch (RejectedExecutionException ex)
            {
                if (isShutdown())
                    logger.info("Executor has shut down, could not submit {}", name);
                else
                    logger.error("Failed to submit {}", name, ex);

                return Futures.immediateCancelledFuture();
            }
        }
    }

    private static class ValidationExecutor extends CompactionExecutor
    {
        public ValidationExecutor()
        {
            super(1, Integer.MAX_VALUE, "ValidationExecutor", new SynchronousQueue<Runnable>());
        }
    }

    private static class CacheCleanupExecutor extends CompactionExecutor
    {
        public CacheCleanupExecutor()
        {
            super(1, "CacheCleanupExecutor");
        }
    }

    public interface CompactionExecutorStatsCollector
    {
        void beginCompaction(CompactionInfo.Holder ci);

        void finishCompaction(CompactionInfo.Holder ci);
    }

    public List<Map<String, String>> getCompactions()
    {
        List<Holder> compactionHolders = CompactionMetrics.getCompactions();
        List<Map<String, String>> out = new ArrayList<Map<String, String>>(compactionHolders.size());
        for (CompactionInfo.Holder ci : compactionHolders)
            out.add(ci.getCompactionInfo().asMap());
        return out;
    }

    public List<String> getCompactionSummary()
    {
        List<Holder> compactionHolders = CompactionMetrics.getCompactions();
        List<String> out = new ArrayList<String>(compactionHolders.size());
        for (CompactionInfo.Holder ci : compactionHolders)
            out.add(ci.getCompactionInfo().toString());
        return out;
    }

    public TabularData getCompactionHistory()
    {
        try
        {
            return SystemKeyspace.getCompactionHistory();
        }
        catch (OpenDataException e)
        {
            throw new RuntimeException(e);
        }
    }

    public long getTotalBytesCompacted()
    {
        return metrics.bytesCompacted.getCount();
    }

    public long getTotalCompactionsCompleted()
    {
        return metrics.totalCompactionsCompleted.getCount();
    }

    public int getPendingTasks()
    {
        return metrics.pendingTasks.getValue();
    }

    public long getCompletedTasks()
    {
        return metrics.completedTasks.getValue();
    }

    private static class CleanupInfo extends CompactionInfo.Holder
    {
        private final SSTableReader sstable;
        private final ISSTableScanner scanner;
        private final UUID cleanupCompactionId;

        public CleanupInfo(SSTableReader sstable, ISSTableScanner scanner)
        {
            this.sstable = sstable;
            this.scanner = scanner;
            cleanupCompactionId = UUIDGen.getTimeUUID();
        }

        public CompactionInfo getCompactionInfo()
        {
            try
            {
                return new CompactionInfo(sstable.metadata,
                                          OperationType.CLEANUP,
                                          scanner.getCurrentPosition(),
                                          scanner.getLengthInBytes(),
                                          cleanupCompactionId);
            }
            catch (Exception e)
            {
                throw new RuntimeException();
            }
        }
    }

    public void stopAllCompactions() {
        for (OperationType type : STOPPABLE_COMPACTION_TYPES) {
            logger.info("Stopping compactions of type {}", type.name());
            stopCompaction(type.name());
        }
        logger.info("All compactions stopped");
    }

    public void stopCompaction(String type)
    {
        OperationType operation = OperationType.valueOf(type);
        for (Holder holder : CompactionMetrics.getCompactions())
        {
            if (holder.getCompactionInfo().getTaskType() == operation)
                holder.stop();
        }
    }

    public void stopCompactionById(String compactionId)
    {
        for (Holder holder : CompactionMetrics.getCompactions())
        {
            UUID holderId = holder.getCompactionInfo().compactionId();
            if (holderId != null && holderId.equals(UUID.fromString(compactionId)))
                holder.stop();
        }
    }

    public int getCoreCompactorThreads()
    {
        return executor.getCorePoolSize();
    }

    public void setCoreCompactorThreads(int number)
    {
        executor.setCorePoolSize(number);
    }

    public int getMaximumCompactorThreads()
    {
        return executor.getMaximumPoolSize();
    }

    public void setMaximumCompactorThreads(int number)
    {
        executor.setMaximumPoolSize(number);
    }

    public int getCoreValidationThreads()
    {
        return validationExecutor.getCorePoolSize();
    }

    public void setCoreValidationThreads(int number)
    {
        validationExecutor.setCorePoolSize(number);
    }

    public int getMaximumValidatorThreads()
    {
        return validationExecutor.getMaximumPoolSize();
    }

    public void setMaximumValidatorThreads(int number)
    {
        validationExecutor.setMaximumPoolSize(number);
    }

    /**
     * Try to stop all of the compactions for given ColumnFamilies.
     *
     * Note that this method does not wait for all compactions to finish; you'll need to loop against
     * isCompacting if you want that behavior.
     *
     * @param columnFamilies The ColumnFamilies to try to stop compaction upon.
     * @param interruptValidation true if validation operations for repair should also be interrupted
     *
     */
    public void interruptCompactionFor(Iterable<CFMetaData> columnFamilies, boolean interruptValidation)
    {
        assert columnFamilies != null;

        // interrupt in-progress compactions
        for (Holder compactionHolder : CompactionMetrics.getCompactions())
        {
            CompactionInfo info = compactionHolder.getCompactionInfo();
            if ((info.getTaskType() == OperationType.VALIDATION) && !interruptValidation)
                continue;

            // cfmetadata is null for index summary redistributions which are 'global' - they involve all keyspaces/tables
            if (info.getCFMetaData() == null || Iterables.contains(columnFamilies, info.getCFMetaData()))
                compactionHolder.stop(); // signal compaction to stop
        }
    }

    public void interruptCompactionForCFs(Iterable<ColumnFamilyStore> cfss, boolean interruptValidation)
    {
        List<CFMetaData> metadata = new ArrayList<>();
        for (ColumnFamilyStore cfs : cfss)
            metadata.add(cfs.metadata);

        interruptCompactionFor(metadata, interruptValidation);
    }

    public void waitForCessation(Iterable<ColumnFamilyStore> cfss)
    {
        long start = System.nanoTime();
        long delay = TimeUnit.MINUTES.toNanos(1);
        while (System.nanoTime() - start < delay)
        {
            if (CompactionManager.instance.isCompacting(cfss))
                Uninterruptibles.sleepUninterruptibly(1, TimeUnit.MILLISECONDS);
            else
                break;
        }
    }
}
