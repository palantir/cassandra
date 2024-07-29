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
    private static final long THROTTLER_TABLE_SIZE_BYTES = Long.getLong("palantir_cassandra.throttler_table_size_gb", 10L) * 1024L * 1024L * 1024L;
    private static final Logger logger = LoggerFactory.getLogger(CompactionThroughputThrottler.class);

    public static RateLimiter getRateLimiter(String keyspace, String columnFamily)
    {
        long liveSpaceUsedBytes = ColumnFamilyStore.getIfExists(keyspace, columnFamily).metric.liveDiskSpaceUsed.getCount();
        if (liveSpaceUsedBytes < THROTTLER_TABLE_SIZE_BYTES)
        {
            logger.trace("Using default compaction throughput {} with rate limit of {}",
                         SafeArg.of("defaultCompactionThroughput", StorageService.instance.getCompactionThroughputMbPerSec()),
                         SafeArg.of("defaultRateLimiter", CompactionManager.instance.getRateLimiter()));

            return CompactionManager.instance.getRateLimiter();
        }

        RateLimiter recommendedCompactionThroughputMbPerSec;
        double diskUsage = Directories.getMaxPathToUtilization().getValue();

        if (diskUsage >= 0.90d)
        {
            // 20 mb/s
            recommendedCompactionThroughputMbPerSec = RateLimiter.create(20d * 1024 * 1024);
        }
        else if (diskUsage >= 0.85d)
        {
            // 40 mb/s
            recommendedCompactionThroughputMbPerSec = RateLimiter.create(40d * 1024 * 1024);
        }
        else if (diskUsage >= 0.80d)
        {
            // 80 mb/s
            recommendedCompactionThroughputMbPerSec = RateLimiter.create(80d * 1024 * 1024);
        }
        else {
            // default
            recommendedCompactionThroughputMbPerSec = RateLimiter.create(CompactionManager.instance.getRateLimiter().getRate());
        }

        logger.debug("Due to disk usage of {} recommending througput of {}MBs with rate limit of {} for keyspace/table {}/{}. " +
                     "Overwritten defaults were throughput {}MBs and rate limit  {}.",
                     SafeArg.of("diskUsage", diskUsage),
                     SafeArg.of("recommendedCompactionThroughputMbPerSec", recommendedCompactionThroughputMbPerSec.getRate() / 1024/ 1024),
                     SafeArg.of("recommendedRateLimiter", recommendedCompactionThroughputMbPerSec),
                     SafeArg.of("defaultCompactionThroughput", StorageService.instance.getCompactionThroughputMbPerSec()),
                     SafeArg.of("defaultRateLimiter", CompactionManager.instance.getRateLimiter()),
                     SafeArg.of("keyspace", keyspace),
                     SafeArg.of("table", columnFamily));

        return recommendedCompactionThroughputMbPerSec;
    }
}
