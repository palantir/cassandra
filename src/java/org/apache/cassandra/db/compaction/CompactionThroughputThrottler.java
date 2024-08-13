package org.apache.cassandra.db.compaction;

import com.google.common.util.concurrent.RateLimiter;
import com.palantir.logsafe.SafeArg;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.service.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Backpressure mechanism to reduce compaction throughput for large tables as disk fills up
 */
public class CompactionThroughputThrottler
{
    private static final boolean ENABLE_COMPACTION_THROUGHPUT_THROTTLING = Boolean.getBoolean("palantir_cassandra.enable_compaction_throughput_throttling");
    private static final long THROTTLER_TABLE_SIZE_BYTES = Long.getLong("palantir_cassandra.throttler_table_size_gb", 10L) * 1024L * 1024L * 1024L;
    private static final Logger logger = LoggerFactory.getLogger(CompactionThroughputThrottler.class);

    public static RateLimiter getRateLimiter(String keyspace, String columnFamily)
    {
        RateLimiter defaultThroughput = RateLimiter.create(CompactionManager.instance.getRateLimiter().getRate());
        if (!ENABLE_COMPACTION_THROUGHPUT_THROTTLING)
        {
            return defaultThroughput;
        }

        double diskUsage = Directories.getMaxPathToUtilization().getValue();
        long liveSpaceUsedBytes = ColumnFamilyStore.getIfExists(keyspace, columnFamily).metric.liveDiskSpaceUsed.getCount();

        if (liveSpaceUsedBytes < THROTTLER_TABLE_SIZE_BYTES || diskUsage < 0.80d)
        {
            logger.trace("No disk pressure or table {} too small {} - using default compaction throughput {} with rate limit of {} for disk usage {}.",
                         SafeArg.of("ksCfPair", String.format("%s/%s", keyspace, columnFamily)),
                         SafeArg.of("liveSpaceUsedBytes", liveSpaceUsedBytes),
                         SafeArg.of("defaultCompactionThroughputMbPerSec", StorageService.instance.getCompactionThroughputMbPerSec()),
                         SafeArg.of("defaultThroughput", defaultThroughput),
                         SafeArg.of("diskUsage", diskUsage));
            return defaultThroughput;
        }

        RateLimiter recommendedThroughput = defaultThroughput;
        if (diskUsage >= 0.90d)
        {
            // 12.5% throughput - at least 10MB/s
            recommendedThroughput = RateLimiter.create(Math.max(defaultThroughput.getRate() / 8, 10d * 1024 * 1024));
        }
        else if (diskUsage >= 0.85d)
        {
            // 25% throughput - at least 20MB/s
            recommendedThroughput = RateLimiter.create(Math.max(defaultThroughput.getRate() / 8, 20d * 1024 * 1024));
        }
        else if (diskUsage >= 0.80d)
        {
            // 50% throughput - at least 40MB/s
            recommendedThroughput = RateLimiter.create(Math.max(defaultThroughput.getRate() / 8, 40d * 1024 * 1024));
        }

        logger.debug("Due to disk usage of {} for table {}, recommending throughput of {}MBs with rate limit of {} for keyspace/table {}/{}. " +
                     "Overwritten defaults were throughput {}MBs and rate limit  {}.",
                     SafeArg.of("ksCfPair", String.format("%s/%s", keyspace, columnFamily)),
                     SafeArg.of("diskUsage", diskUsage),
                     SafeArg.of("recommendedThroughputMbs", recommendedThroughput.getRate() / 1024/ 1024),
                     SafeArg.of("recommendedRateLimiter", recommendedThroughput),
                     SafeArg.of("defaultCompactionThroughput", StorageService.instance.getCompactionThroughputMbPerSec()),
                     SafeArg.of("defaultRateLimiter", CompactionManager.instance.getRateLimiter()),
                     SafeArg.of("keyspace", keyspace),
                     SafeArg.of("table", columnFamily));

        return recommendedThroughput;
    }
}
