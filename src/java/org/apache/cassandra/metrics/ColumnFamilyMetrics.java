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
package org.apache.cassandra.metrics;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.palantir.cassandra.utils.CountingCellIterator;
import com.codahale.metrics.*;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Memtable;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.TopKSampler;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;


/**
 * Metrics for {@link ColumnFamilyStore}.
 */
public class ColumnFamilyMetrics
{

    /** Total amount of data stored in the memtable that resides on-heap, including column related overhead and overwritten rows. */
    public final Gauge<Long> memtableOnHeapSize;
    /** Total amount of data stored in the memtable that resides off-heap, including column related overhead and overwritten rows. */
    public final Gauge<Long> memtableOffHeapSize;
    /** Total amount of live data stored in the memtable, excluding any data structure overhead */
    public final Gauge<Long> memtableLiveDataSize;
    /** Total amount of data stored in the memtables (2i and pending flush memtables included) that resides on-heap. */
    public final Gauge<Long> allMemtablesOnHeapSize;
    /** Total amount of data stored in the memtables (2i and pending flush memtables included) that resides off-heap. */
    public final Gauge<Long> allMemtablesOffHeapSize;
    /** Total amount of live data stored in the memtables (2i and pending flush memtables included) that resides off-heap, excluding any data structure overhead */
    public final Gauge<Long> allMemtablesLiveDataSize;
    /** Total number of columns present in the memtable. */
    public final Gauge<Long> memtableColumnsCount;
    /** Number of times flush has resulted in the memtable being switched out. */
    public final Counter memtableSwitchCount;
    /** Current compression ratio for all SSTables */
    public final Gauge<Double> compressionRatio;
    /** Histogram of estimated row size (in bytes). */
    public final Gauge<long[]> estimatedRowSizeHistogram;
    /** Approximate number of keys in table. */
    public final Gauge<Long> estimatedRowCount;
    /** Histogram of estimated number of columns. */
    public final Gauge<long[]> estimatedColumnCountHistogram;
    /** Histogram of the number of sstable data files accessed per read */
    public final ColumnFamilyHistogram sstablesPerReadHistogram;
    /** (Local) read metrics */
    public final LatencyMetrics readLatency;
    /** (Local) range slice metrics */
    public final LatencyMetrics rangeLatency;
    /** (Local) write metrics */
    public final LatencyMetrics writeLatency;
    /** Estimated number of tasks pending for this column family */
    public final Counter pendingFlushes;
    /** Total number of bytes flushed since server [re]start */
    public final Counter bytesFlushed;
    /** Total number of bytes written by compaction since server [re]start */
    public final Counter compactionBytesWritten;
    /** Total number of compactions since server [re]start */
    public final Counter compactionsCompleted;
    /** Estimate of number of pending compactios for this table */
    public final Gauge<Integer> pendingCompactions;
    /** Number of SSTables that have a repairedAt timestamp > 0 (from incremental repairs) */
    public final Gauge<Long> repairedAtSSTableCount;
    /** Number of SSTables on disk for this CF */
    public final Gauge<Integer> liveSSTableCount;
    /** Disk space used by SSTables belonging to this CF */
    public final Counter liveDiskSpaceUsed;
    /** Total disk space used by SSTables belonging to this CF, including obsolete ones waiting to be GC'd */
    public final Counter totalDiskSpaceUsed;
    /** Size of the smallest compacted row */
    public final Gauge<Long> minRowSize;
    /** Size of the largest compacted row */
    public final Gauge<Long> maxRowSize;
    /** Size of the smallest compacted row */
    public final Gauge<Long> meanRowSize;
    /** Number of false positives in bloom filter */
    public final Gauge<Long> bloomFilterFalsePositives;
    /** Number of false positives in bloom filter from last read */
    public final Gauge<Long> recentBloomFilterFalsePositives;
    /** False positive ratio of bloom filter */
    public final Gauge<Double> bloomFilterFalseRatio;
    /** False positive ratio of bloom filter from last read */
    public final Gauge<Double> recentBloomFilterFalseRatio;
    /** Disk space used by bloom filter */
    public final Gauge<Long> bloomFilterDiskSpaceUsed;
    /** Off heap memory used by bloom filter */
    public final Gauge<Long> bloomFilterOffHeapMemoryUsed;
    /** Off heap memory used by index summary */
    public final Gauge<Long> indexSummaryOffHeapMemoryUsed;
    /** Off heap memory used by compression meta data*/
    public final Gauge<Long> compressionMetadataOffHeapMemoryUsed;
    /** Key cache hit rate  for this CF */
    public final Gauge<Double> keyCacheHitRate;
    /** Rate of tombstones read */
    public final Meter droppableTombstones;
    /** Tombstones scanned in queries on this CF */
    public final ColumnFamilyHistogram tombstoneScannedHistogram;
    /** Live cells scanned in queries on this CF */
    public final ColumnFamilyHistogram liveScannedHistogram;
    /** Droppable tombstones scanned in queries on this CF per {@link CountingCellIterator} */
    public final ColumnFamilyHistogram droppableTombstonesReadHistogram;
    /** Droppable ttls scanned in queries on this CF per {@link CountingCellIterator} */
    public final ColumnFamilyHistogram droppableTtlsReadHistogram;
    /** Live cells scanned in queries on this CF per {@link CountingCellIterator} */
    public final ColumnFamilyHistogram liveReadHistogram;
    /** Tombstones scanned in queries on this CF per {@link CountingCellIterator} */
    public final ColumnFamilyHistogram tombstonesReadHistogram;
    /** Range tombstones scanned in queries on this CF per {@link com.palantir.cassandra.utils.RangeTombstoneCountingIterator} */
    public final ColumnFamilyHistogram rangeTombstonesReadHistogram;
    /** Droppable range tombstones scanned in queries on this CF per {@link com.palantir.cassandra.utils.RangeTombstoneCountingIterator} */
    public final ColumnFamilyHistogram droppableRangeTombstonesReadHistogram;
    /** Range tombstones held in memory when performing a read on this CF */
    public final ColumnFamilyHistogram rangeTombstonesHistogram;
    /** Number of tombstone read failures */
    public final Counter rowCountFailures;
    /** Number of tombstone read warnings */
    public final Counter rowCountWarnings;
    /** Number of tombstone read failures */
    public final Counter tombstoneFailures;
    /** Number of tombstone read warnings */
    public final Counter tombstoneWarnings;
    /** Column update time delta on this CF */
    public final ColumnFamilyHistogram colUpdateTimeDeltaHistogram;
    /** Disk space used by snapshot files which */
    public final Gauge<Long> trueSnapshotsSize;
    /** Row cache hits, but result out of range */
    public final Counter rowCacheHitOutOfRange;
    /** Number of row cache hits */
    public final Counter rowCacheHit;
    /** Number of row cache misses */
    public final Counter rowCacheMiss;
    /** CAS Prepare metrics */
    public final LatencyMetrics casPrepare;
    /** CAS Propose metrics */
    public final LatencyMetrics casPropose;
    /** CAS Commit metrics */
    public final LatencyMetrics casCommit;

    /** Estimated ratio of droppable tombstones and number of cells in this table */
    public final Gauge<Double> droppableTombstoneRatio;
    /** Estimated ratio of live tombstones and number of cells in this table */
    public final Gauge<Double> liveTombstoneRatio;
    /** Estimated ratio of tombstones and number of cells in this table */
    public final Gauge<Double> tombstoneRatio;
    /** Estimated count of droppable tombstones and number of cells in this table */
    public final Gauge<Double> droppableTombstoneCount;
    /** Estimated count of live tombstones and number of cells in this table */
    public final Gauge<Double> liveTombstoneCount;
    /** Estimated count of tombstones and number of cells in this table */
    public final Gauge<Double> tombstoneCount;

    /** Bytes read on range scans **/
    public final Meter rangeScanBytesRead;
    /** Bytes read on reads **/
    public final Meter readBytesRead;

    public final LatencyMetrics coordinatorReadLatency;
    public final LatencyMetrics coordinatorReadScanLatency;
    public final LatencyMetrics coordinatorScanLatency;

    public final LatencyMetrics blockingReadRepairLatency;
    public final Meter blockingReadRepairs;
    public final Meter attemptedReadRepairs;
    public final Meter backgroundReadRepairs;

    public final Counter largePartitionsCompacted;

    /** Request rounds in range scan queries on this CF **/
    public final ColumnFamilyHistogram coordinatorScanRequestRounds;

    /** Number of separate requests in a range scan query on this CF **/
    public final ColumnFamilyHistogram coordinatorScanRequestQueries;

    /** Number of token ranges in a range scan query on this CF **/
    public final ColumnFamilyHistogram coordinatorScanRequestTokenRanges;

    /** Time spent waiting for free memtable space, either on- or off-heap */
    public final Histogram waitingOnFreeMemtableSpace;

    private final MetricNameFactory factory;
    private static final MetricNameFactory globalNameFactory = new AllColumnFamilyMetricNameFactory();

    public final Counter speculativeRetries;

    public final static LatencyMetrics globalReadLatency = new LatencyMetrics(globalNameFactory, "Read");
    public final static LatencyMetrics globalWriteLatency = new LatencyMetrics(globalNameFactory, "Write");
    public final static LatencyMetrics globalRangeLatency = new LatencyMetrics(globalNameFactory, "Range");
    
    public final Map<Sampler, TopKSampler<ByteBuffer>> samplers;
    /**
     * stores metrics that will be rolled into a single global metric
     */
    public final static ConcurrentMap<String, Set<Metric>> allColumnFamilyMetrics = Maps.newConcurrentMap();
    
    /**
     * Stores all metric names created that can be used when unregistering
     */
    public final static Set<String> all = Sets.newHashSet();

    private interface GetHistogram
    {
        public EstimatedHistogram getHistogram(SSTableReader reader);
    }

    private static long[] combineHistograms(Iterable<SSTableReader> sstables, GetHistogram getHistogram)
    {
        Iterator<SSTableReader> iterator = sstables.iterator();
        if (!iterator.hasNext())
        {
            return new long[0];
        }
        long[] firstBucket = getHistogram.getHistogram(iterator.next()).getBuckets(false);
        long[] values = new long[firstBucket.length];
        System.arraycopy(firstBucket, 0, values, 0, values.length);

        while (iterator.hasNext())
        {
            long[] nextBucket = getHistogram.getHistogram(iterator.next()).getBuckets(false);
            if (nextBucket.length > values.length)
            {
                long[] newValues = new long[nextBucket.length];
                System.arraycopy(firstBucket, 0, newValues, 0, firstBucket.length);
                for (int i = 0; i < newValues.length; i++)
                {
                    newValues[i] += nextBucket[i];
                }
                values = newValues;
            }
            else
            {
                for (int i = 0; i < values.length; i++)
                {
                    values[i] += nextBucket[i];
                }
            }
        }
        return values;
    }
    
    /**
     * Creates metrics for given {@link ColumnFamilyStore}.
     *
     * @param cfs ColumnFamilyStore to measure metrics
     */
    public ColumnFamilyMetrics(final ColumnFamilyStore cfs)
    {
        factory = new ColumnFamilyMetricNameFactory(cfs);

        samplers = Maps.newHashMap();
        for (Sampler sampler : Sampler.values())
        {
            samplers.put(sampler, new TopKSampler<ByteBuffer>());
        }

        memtableColumnsCount = createColumnFamilyGauge("MemtableColumnsCount", new Gauge<Long>()
        {
            public Long getValue()
            {
                return cfs.getTracker().getView().getCurrentMemtable().getOperations();
            }
        });
        memtableOnHeapSize = createColumnFamilyGauge("MemtableOnHeapSize", new Gauge<Long>()
        {
            public Long getValue()
            {
                return cfs.getTracker().getView().getCurrentMemtable().getAllocator().onHeap().owns();
            }
        });
        memtableOffHeapSize = createColumnFamilyGauge("MemtableOffHeapSize", new Gauge<Long>()
        {
            public Long getValue()
            {
                return cfs.getTracker().getView().getCurrentMemtable().getAllocator().offHeap().owns();
            }
        });
        memtableLiveDataSize = createColumnFamilyGauge("MemtableLiveDataSize", new Gauge<Long>()
        {
            public Long getValue()
            {
                return cfs.getTracker().getView().getCurrentMemtable().getLiveDataSize();
            }
        });
        allMemtablesOnHeapSize = createColumnFamilyGauge("AllMemtablesHeapSize", new Gauge<Long>()
        {
            public Long getValue()
            {
                long size = 0;
                for (ColumnFamilyStore cfs2 : cfs.concatWithIndexes())
                    size += cfs2.getTracker().getView().getCurrentMemtable().getAllocator().onHeap().owns();
                return size;
            }
        });
        allMemtablesOffHeapSize = createColumnFamilyGauge("AllMemtablesOffHeapSize", new Gauge<Long>()
        {
            public Long getValue()
            {
                long size = 0;
                for (ColumnFamilyStore cfs2 : cfs.concatWithIndexes())
                    size += cfs2.getTracker().getView().getCurrentMemtable().getAllocator().offHeap().owns();
                return size;
            }
        });
        allMemtablesLiveDataSize = createColumnFamilyGauge("AllMemtablesLiveDataSize", new Gauge<Long>()
        {
            public Long getValue()
            {
                long size = 0;
                for (ColumnFamilyStore cfs2 : cfs.concatWithIndexes())
                    size += cfs2.getTracker().getView().getCurrentMemtable().getLiveDataSize();
                return size;
            }
        });
        memtableSwitchCount = createColumnFamilyCounter("MemtableSwitchCount");
        estimatedRowSizeHistogram = Metrics.register(factory.createMetricName("EstimatedRowSizeHistogram"), new Gauge<long[]>()
        {
            public long[] getValue()
            {
                return combineHistograms(cfs.getSSTables(), new GetHistogram()
                {
                    public EstimatedHistogram getHistogram(SSTableReader reader)
                    {
                        return reader.getEstimatedRowSize();
                    }
                });
            }
        });
        estimatedRowCount = Metrics.register(factory.createMetricName("EstimatedRowCount"), new Gauge<Long>()
        {
            public Long getValue()
            {
                long memtablePartitions = 0;
                for (Memtable memtable : cfs.getTracker().getView().getAllMemtables())
                    memtablePartitions += memtable.partitionCount();
                return SSTableReader.getApproximateKeyCount(cfs.getSSTables()) + memtablePartitions;
            }
        });
        estimatedColumnCountHistogram = Metrics.register(factory.createMetricName("EstimatedColumnCountHistogram"), new Gauge<long[]>()
        {
            public long[] getValue()
            {
                return combineHistograms(cfs.getSSTables(), new GetHistogram()
                {
                    public EstimatedHistogram getHistogram(SSTableReader reader)
                    {
                        return reader.getEstimatedColumnCount();
                    }
                });
            }
        });
        sstablesPerReadHistogram = createColumnFamilyHistogram("SSTablesPerReadHistogram", cfs.keyspace.metric.sstablesPerReadHistogram, true);
        compressionRatio = createColumnFamilyGauge("CompressionRatio", new Gauge<Double>()
        {
            public Double getValue()
            {
                double sum = 0;
                int total = 0;
                for (SSTableReader sstable : cfs.getSSTables())
                {
                    if (sstable.getCompressionRatio() != MetadataCollector.NO_COMPRESSION_RATIO)
                    {
                        sum += sstable.getCompressionRatio();
                        total++;
                    }
                }
                return total != 0 ? sum / total : 0;
            }
        }, new Gauge<Double>() // global gauge
        {
            public Double getValue()
            {
                double sum = 0;
                int total = 0;
                for (Keyspace keyspace : Keyspace.all())
                {
                    for (SSTableReader sstable : keyspace.getAllSSTables())
                    {
                        if (sstable.getCompressionRatio() != MetadataCollector.NO_COMPRESSION_RATIO)
                        {
                            sum += sstable.getCompressionRatio();
                            total++;
                        }
                    }
                }
                return total != 0 ? sum / total : 0;
            }
        });
        readLatency = new LatencyMetrics(factory, "Read", cfs.keyspace.metric.readLatency, globalReadLatency);
        writeLatency = new LatencyMetrics(factory, "Write", cfs.keyspace.metric.writeLatency, globalWriteLatency);
        rangeLatency = new LatencyMetrics(factory, "Range", cfs.keyspace.metric.rangeLatency, globalRangeLatency);
        coordinatorReadLatency = new LatencyMetrics(factory, "CoordinatorRead",  new LatencyMetrics(globalNameFactory, "CoordinatorRead"));
        coordinatorReadScanLatency = new LatencyMetrics(factory, "CoordinatorReadScan",  new LatencyMetrics(globalNameFactory, "CoordinatorReadScan"));
        coordinatorScanLatency = new LatencyMetrics(factory, "CoordinatorScan", new LatencyMetrics(globalNameFactory, "CoordinatorScan"));
        blockingReadRepairLatency = new LatencyMetrics(factory, "BlockingReadRepair", new LatencyMetrics(globalNameFactory, "BlockingReadRepair"));
        blockingReadRepairs = Metrics.meter(factory.createMetricName("BlockingReadRepairs"));
        backgroundReadRepairs = Metrics.meter(factory.createMetricName("BackgroundReadRepairs"));
        attemptedReadRepairs = Metrics.meter(factory.createMetricName("AttemptedReadRepairs"));
        largePartitionsCompacted = Metrics.counter("LargePartitionsCompacted");
        pendingFlushes = createColumnFamilyCounter("PendingFlushes");
        bytesFlushed = createColumnFamilyCounter("BytesFlushed");
        compactionBytesWritten = createColumnFamilyCounter("CompactionBytesWritten");
        compactionsCompleted = createColumnFamilyCounter("CompactionsCompleted");
        pendingCompactions = createColumnFamilyGauge("PendingCompactions", new Gauge<Integer>()
        {
            public Integer getValue()
            {
                return cfs.getCompactionStrategy().getEstimatedRemainingTasks();
            }
        });
        liveSSTableCount = createColumnFamilyGauge("LiveSSTableCount", new Gauge<Integer>()
        {
            public Integer getValue()
            {
                return cfs.getTracker().getSSTables().size();
            }
        });
        repairedAtSSTableCount = createColumnFamilyGauge("RepairedAtSSTableCount", new Gauge<Long>()
        {
            public Long getValue()
            {
                long repairedAtCount = 0;
                for (Long repairedAt : cfs.getRepairedAtPerSstable().values())
                {
                    if (repairedAt > ActiveRepairService.UNREPAIRED_SSTABLE)
                        repairedAtCount++;
                }
                return repairedAtCount;
            }
        });
        liveDiskSpaceUsed = createColumnFamilyCounter("LiveDiskSpaceUsed");
        totalDiskSpaceUsed = createColumnFamilyCounter("TotalDiskSpaceUsed");
        minRowSize = createColumnFamilyGauge("MinRowSize", new Gauge<Long>()
        {
            public Long getValue()
            {
                long min = 0;
                for (SSTableReader sstable : cfs.getSSTables())
                {
                    if (min == 0 || sstable.getEstimatedRowSize().min() < min)
                        min = sstable.getEstimatedRowSize().min();
                }
                return min;
            }
        }, new Gauge<Long>() // global gauge
        {
            public Long getValue()
            {
                long min = Long.MAX_VALUE;
                for (Metric cfGauge : allColumnFamilyMetrics.get("MinRowSize"))
                {
                    min = Math.min(min, ((Gauge<? extends Number>) cfGauge).getValue().longValue());
                }
                return min;
            }
        });
        maxRowSize = createColumnFamilyGauge("MaxRowSize", new Gauge<Long>()
        {
            public Long getValue()
            {
                long max = 0;
                for (SSTableReader sstable : cfs.getSSTables())
                {
                    if (sstable.getEstimatedRowSize().max() > max)
                        max = sstable.getEstimatedRowSize().max();
                }
                return max;
            }
        }, new Gauge<Long>() // global gauge
        {
            public Long getValue()
            {
                long max = 0;
                for (Metric cfGauge : allColumnFamilyMetrics.get("MaxRowSize"))
                {
                    max = Math.max(max, ((Gauge<? extends Number>) cfGauge).getValue().longValue());
                }
                return max;
            }
        });
        meanRowSize = createColumnFamilyGauge("MeanRowSize", new Gauge<Long>()
        {
            public Long getValue()
            {
                long sum = 0;
                long count = 0;
                for (SSTableReader sstable : cfs.getSSTables())
                {
                    long n = sstable.getEstimatedRowSize().count();
                    sum += sstable.getEstimatedRowSize().mean() * n;
                    count += n;
                }
                return count > 0 ? sum / count : 0;
            }
        }, new Gauge<Long>() // global gauge
        {
            public Long getValue()
            {
                long sum = 0;
                long count = 0;
                for (Keyspace keyspace : Keyspace.all())
                {
                    for (SSTableReader sstable : keyspace.getAllSSTables())
                    {
                        long n = sstable.getEstimatedRowSize().count();
                        sum += sstable.getEstimatedRowSize().mean() * n;
                        count += n;
                    }
                }
                return count > 0 ? sum / count : 0;
            }
        });
        bloomFilterFalsePositives = createColumnFamilyGauge("BloomFilterFalsePositives", new Gauge<Long>()
        {
            public Long getValue()
            {
                long count = 0L;
                for (SSTableReader sstable: cfs.getSSTables())
                    count += sstable.getBloomFilterFalsePositiveCount();
                return count;
            }
        });
        recentBloomFilterFalsePositives = createColumnFamilyGauge("RecentBloomFilterFalsePositives", new Gauge<Long>()
        {
            public Long getValue()
            {
                long count = 0L;
                for (SSTableReader sstable : cfs.getSSTables())
                    count += sstable.getRecentBloomFilterFalsePositiveCount();
                return count;
            }
        });
        bloomFilterFalseRatio = createColumnFamilyGauge("BloomFilterFalseRatio", new Gauge<Double>()
        {
            public Double getValue()
            {
                long falseCount = 0L;
                long trueCount = 0L;
                for (SSTableReader sstable : cfs.getSSTables())
                {
                    falseCount += sstable.getBloomFilterFalsePositiveCount();
                    trueCount += sstable.getBloomFilterTruePositiveCount();
                }
                if (falseCount == 0L && trueCount == 0L)
                    return 0d;
                return (double) falseCount / (trueCount + falseCount);
            }
        }, new Gauge<Double>() // global gauge
        {
            public Double getValue()
            {
                long falseCount = 0L;
                long trueCount = 0L;
                for (Keyspace keyspace : Keyspace.all())
                {
                    for (SSTableReader sstable : keyspace.getAllSSTables())
                    {
                        falseCount += sstable.getBloomFilterFalsePositiveCount();
                        trueCount += sstable.getBloomFilterTruePositiveCount();
                    }
                }
                if (falseCount == 0L && trueCount == 0L)
                    return 0d;
                return (double) falseCount / (trueCount + falseCount);
            }
        });
        recentBloomFilterFalseRatio = createColumnFamilyGauge("RecentBloomFilterFalseRatio", new Gauge<Double>()
        {
            public Double getValue()
            {
                long falseCount = 0L;
                long trueCount = 0L;
                for (SSTableReader sstable: cfs.getSSTables())
                {
                    falseCount += sstable.getRecentBloomFilterFalsePositiveCount();
                    trueCount += sstable.getRecentBloomFilterTruePositiveCount();
                }
                if (falseCount == 0L && trueCount == 0L)
                    return 0d;
                return (double) falseCount / (trueCount + falseCount);
            }
        }, new Gauge<Double>() // global gauge
        {
            public Double getValue()
            {
                long falseCount = 0L;
                long trueCount = 0L;
                for (Keyspace keyspace : Keyspace.all())
                {
                    for (SSTableReader sstable : keyspace.getAllSSTables())
                    {
                        falseCount += sstable.getRecentBloomFilterFalsePositiveCount();
                        trueCount += sstable.getRecentBloomFilterTruePositiveCount();
                    }
                }
                if (falseCount == 0L && trueCount == 0L)
                    return 0d;
                return (double) falseCount / (trueCount + falseCount);
            }
        });
        bloomFilterDiskSpaceUsed = createColumnFamilyGauge("BloomFilterDiskSpaceUsed", new Gauge<Long>()
        {
            public Long getValue()
            {
                long total = 0;
                for (SSTableReader sst : cfs.getSSTables())
                    total += sst.getBloomFilterSerializedSize();
                return total;
            }
        });
        bloomFilterOffHeapMemoryUsed = createColumnFamilyGauge("BloomFilterOffHeapMemoryUsed", new Gauge<Long>()
        {
            public Long getValue()
            {
                long total = 0;
                for (SSTableReader sst : cfs.getSSTables())
                    total += sst.getBloomFilterOffHeapSize();
                return total;
            }
        });
        indexSummaryOffHeapMemoryUsed = createColumnFamilyGauge("IndexSummaryOffHeapMemoryUsed", new Gauge<Long>()
        {
            public Long getValue()
            {
                long total = 0;
                for (SSTableReader sst : cfs.getSSTables())
                    total += sst.getIndexSummaryOffHeapSize();
                return total;
            }
        });
        compressionMetadataOffHeapMemoryUsed = createColumnFamilyGauge("CompressionMetadataOffHeapMemoryUsed", new Gauge<Long>()
        {
            public Long getValue()
            {
                long total = 0;
                for (SSTableReader sst : cfs.getSSTables())
                    total += sst.getCompressionMetadataOffHeapSize();
                return total;
            }
        });
        speculativeRetries = createColumnFamilyCounter("SpeculativeRetries");
        keyCacheHitRate = Metrics.register(factory.createMetricName("KeyCacheHitRate"), new RatioGauge()
        {
            @Override
            public Ratio getRatio()
            {
                return Ratio.of(getNumerator(), getDenominator());
            }

            protected double getNumerator()
            {
                long hits = 0L;
                for (SSTableReader sstable : cfs.getSSTables())
                    hits += sstable.getKeyCacheHit();
                return hits;
            }

            protected double getDenominator()
            {
                long requests = 0L;
                for (SSTableReader sstable : cfs.getSSTables())
                    requests += sstable.getKeyCacheRequest();
                return Math.max(requests, 1); // to avoid NaN.
            }
        });
        tombstoneScannedHistogram = createColumnFamilyHistogram("TombstoneScannedHistogram", cfs.keyspace.metric.tombstoneScannedHistogram, false);
        liveScannedHistogram = createColumnFamilyHistogram("LiveScannedHistogram", cfs.keyspace.metric.liveScannedHistogram, false);
        droppableTombstonesReadHistogram = createColumnFamilyHistogram("DroppableTombstonesReadHistogram", cfs.keyspace.metric.droppableTombstonesReadHistogram, false);
        droppableTtlsReadHistogram = createColumnFamilyHistogram("DroppableTtlsReadHistogram", cfs.keyspace.metric.droppableTtlsReadHistogram, false);
        liveReadHistogram = createColumnFamilyHistogram("LiveReadHistogram", cfs.keyspace.metric.liveReadHistogram, false);
        tombstonesReadHistogram = createColumnFamilyHistogram("TombstonesReadHistogram", cfs.keyspace.metric.tombstonesReadHistogram, false);
        rangeTombstonesReadHistogram = createColumnFamilyHistogram("RangeTombstonesReadHistogram", cfs.keyspace.metric.rangeTombstonesReadHistogram, false);
        droppableRangeTombstonesReadHistogram = createColumnFamilyHistogram("DroppableRangeTombstonesReadHistogram", cfs.keyspace.metric.droppableRangeTombstonesReadHistogram, false);
        rangeTombstonesHistogram = createColumnFamilyHistogram("RangeTombstonesHistogram", cfs.keyspace.metric.rangeTombstonesHistogram, false);
        colUpdateTimeDeltaHistogram = createColumnFamilyHistogram("ColUpdateTimeDeltaHistogram", cfs.keyspace.metric.colUpdateTimeDeltaHistogram, false);
        coordinatorScanRequestRounds = createColumnFamilyHistogram("CoordinatorScanRequestRounds", cfs.keyspace.metric.coordinatorScanRequestRounds, false);
        coordinatorScanRequestQueries = createColumnFamilyHistogram("CoordinatorScanRequestQueries", cfs.keyspace.metric.coordinatorScanRequestQueries, false);
        coordinatorScanRequestTokenRanges = createColumnFamilyHistogram("CoordinatorScanRequestTokenRanges", cfs.keyspace.metric.coordinatorScanRequestTokenRanges, false);
        waitingOnFreeMemtableSpace = Metrics.histogram(factory.createMetricName("WaitingOnFreeMemtableSpace"), false);

        trueSnapshotsSize = createColumnFamilyGauge("SnapshotsSize", new Gauge<Long>()
        {
            public Long getValue()
            {
                return cfs.trueSnapshotsSize();
            }
        });
        rowCacheHitOutOfRange = createColumnFamilyCounter("RowCacheHitOutOfRange");
        rowCacheHit = createColumnFamilyCounter("RowCacheHit");
        rowCacheMiss = createColumnFamilyCounter("RowCacheMiss");

        rowCountFailures = createColumnFamilyCounter("RowCountFailures");
        rowCountWarnings = createColumnFamilyCounter("RowCountWarnings");
        tombstoneFailures = createColumnFamilyCounter("TombstoneFailures");
        tombstoneWarnings = createColumnFamilyCounter("TombstoneWarnings");

        casPrepare = new LatencyMetrics(factory, "CasPrepare", cfs.keyspace.metric.casPrepare);
        casPropose = new LatencyMetrics(factory, "CasPropose", cfs.keyspace.metric.casPropose);
        casCommit = new LatencyMetrics(factory, "CasCommit", cfs.keyspace.metric.casCommit);

        droppableTombstoneRatio = createColumnFamilyGauge("DroppableTombstoneRatio", cfs::getDroppableTombstoneRatio);
        liveTombstoneRatio = createColumnFamilyGauge("LiveTombstoneRatio", cfs::getLiveTombstoneRatio);
        tombstoneRatio = createColumnFamilyGauge("TombstoneRatio", cfs::getTombstoneRatio);
        droppableTombstoneCount = createColumnFamilyGauge("DroppableTombstoneCount", cfs::getDroppableTombstoneCount);
        liveTombstoneCount = createColumnFamilyGauge("LiveTombstoneCount", cfs::getLiveTombstoneCount);
        tombstoneCount = createColumnFamilyGauge("TombstoneCount", cfs::getTombstoneCount);

        rangeScanBytesRead = Metrics.meter(factory.createMetricName("RangeScanBytesRead"));
        readBytesRead = Metrics.meter(factory.createMetricName("ReadBytesRead"));
        droppableTombstones = Metrics.meter(factory.createMetricName("DroppableTombstonesRead"));
    }

    public void updateSSTableIterated(int count)
    {
        sstablesPerReadHistogram.update(count);
    }

    /**
     * Release all associated metrics.
     */
    public void release()
    {
        for(String name : all)
        {
            allColumnFamilyMetrics.get(name).remove(Metrics.getMetrics().get(factory.createMetricName(name).getMetricName()));
            Metrics.remove(factory.createMetricName(name));
        }
        readLatency.release();
        writeLatency.release();
        rangeLatency.release();
        Metrics.remove(factory.createMetricName("EstimatedRowSizeHistogram"));
        Metrics.remove(factory.createMetricName("EstimatedRowCount"));
        Metrics.remove(factory.createMetricName("EstimatedColumnCountHistogram"));
        Metrics.remove(factory.createMetricName("KeyCacheHitRate"));
        Metrics.remove(factory.createMetricName("CoordinatorReadLatency"));
        Metrics.remove(factory.createMetricName("CoordinatorScanLatency"));
        Metrics.remove(factory.createMetricName("WaitingOnFreeMemtableSpace"));
    }


    /**
     * Create a gauge that will be part of a merged version of all column families.  The global gauge
     * will merge each CF gauge by adding their values 
     */
    protected <T extends Number> Gauge<T> createColumnFamilyGauge(final String name, Gauge<T> gauge)
    {
        return createColumnFamilyGauge(name, gauge, new Gauge<Long>()
        {
            public Long getValue()
            {
                long total = 0;
                for (Metric cfGauge : allColumnFamilyMetrics.get(name))
                {
                    total = total + ((Gauge<? extends Number>) cfGauge).getValue().longValue();
                }
                return total;
            }
        });
    }
    
    /**
     * Create a gauge that will be part of a merged version of all column families.  The global gauge
     * is defined as the globalGauge parameter
     */
    protected <G,T> Gauge<T> createColumnFamilyGauge(String name, Gauge<T> gauge, Gauge<G> globalGauge)
    {
        Gauge<T> cfGauge = Metrics.register(factory.createMetricName(name), gauge);
        if (register(name, cfGauge))
        {
            Metrics.register(globalNameFactory.createMetricName(name), globalGauge);
        }
        return cfGauge;
    }
    
    /**
     * Creates a counter that will also have a global counter thats the sum of all counters across 
     * different column families
     */
    protected Counter createColumnFamilyCounter(final String name)
    {
        Counter cfCounter = Metrics.counter(factory.createMetricName(name));
        if (register(name, cfCounter))
        {
            Metrics.register(globalNameFactory.createMetricName(name), new Gauge<Long>()
            {
                public Long getValue()
                {
                    long total = 0;
                    for (Metric cfGauge : allColumnFamilyMetrics.get(name))
                    {
                        total += ((Counter) cfGauge).getCount();
                    }
                    return total;
                }
            });
        }
        return cfCounter;
    }
    
    /**
     * Create a histogram-like interface that will register both a CF, keyspace and global level
     * histogram and forward any updates to both
     */
    protected ColumnFamilyHistogram createColumnFamilyHistogram(String name, Histogram keyspaceHistogram, boolean considerZeroes)
    {
        Histogram cfHistogram = Metrics.histogram(factory.createMetricName(name), considerZeroes);
        register(name, cfHistogram);
        return new ColumnFamilyHistogram(cfHistogram, keyspaceHistogram, Metrics.histogram(globalNameFactory.createMetricName(name), considerZeroes));
    }

    /**
     * Registers a metric to be removed when unloading CF.
     * @return true if first time metric with that name has been registered
     */
    private boolean register(String name, Metric metric)
    { 
        boolean ret = allColumnFamilyMetrics.putIfAbsent(name,  new HashSet<Metric>()) == null;
        allColumnFamilyMetrics.get(name).add(metric);
        all.add(name);
        return ret;
    }
    
    public static class ColumnFamilyHistogram
    {
        public final Histogram[] all;
        public final Histogram cf;
        private ColumnFamilyHistogram(Histogram cf, Histogram keyspace, Histogram global)
        {
            this.cf = cf;
            this.all = new Histogram[]{cf, keyspace, global};
        }

        public void update(long i)
        {
            for(Histogram histo : all)
            {
                histo.update(i);
            }
        }
    }
    
    static class ColumnFamilyMetricNameFactory implements MetricNameFactory
    {
        private final String keyspaceName;
        private final String columnFamilyName;
        private final boolean isIndex;

        ColumnFamilyMetricNameFactory(ColumnFamilyStore cfs)
        {
            this.keyspaceName = cfs.keyspace.getName();
            this.columnFamilyName = cfs.name;
            isIndex = cfs.isIndex();
        }

        public CassandraMetricsRegistry.MetricName createMetricName(String metricName)
        {
            String groupName = ColumnFamilyMetrics.class.getPackage().getName();
            String type = isIndex ? "IndexColumnFamily" : "ColumnFamily";

            StringBuilder mbeanName = new StringBuilder();
            mbeanName.append(groupName).append(":");
            mbeanName.append("type=").append(type);
            mbeanName.append(",keyspace=").append(keyspaceName);
            mbeanName.append(",scope=").append(columnFamilyName);
            mbeanName.append(",name=").append(metricName);

            return new CassandraMetricsRegistry.MetricName(groupName, type, metricName, keyspaceName + "." + columnFamilyName, mbeanName.toString());
        }
    }
    
    static class AllColumnFamilyMetricNameFactory implements MetricNameFactory
    {
        public CassandraMetricsRegistry.MetricName createMetricName(String metricName)
        {
            String groupName = ColumnFamilyMetrics.class.getPackage().getName(); 
            StringBuilder mbeanName = new StringBuilder();
            mbeanName.append(groupName).append(":");
            mbeanName.append("type=ColumnFamily");
            mbeanName.append(",name=").append(metricName);
            return new CassandraMetricsRegistry.MetricName(groupName, "ColumnFamily", metricName, "all", mbeanName.toString());
        }
    }

    public static enum Sampler
    {
        READS, WRITES
    }
}
