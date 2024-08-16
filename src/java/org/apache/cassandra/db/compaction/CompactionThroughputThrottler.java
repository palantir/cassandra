package org.apache.cassandra.db.compaction;

import com.google.common.util.concurrent.RateLimiter;
import com.palantir.logsafe.SafeArg;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.Pair;
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

    public static RateLimiter getRateLimiter(Pair<String, String> ksCf)
    {
        String keyspace = ksCf.left;
        String columnFamily = ksCf.right;
        RateLimiter defaultRateLimit = RateLimiter.create(CompactionManager.instance.getRateLimiter().getRate());
        if (!ENABLE_COMPACTION_THROUGHPUT_THROTTLING)
        {
            return defaultRateLimit;
        }

        double diskUsage = Directories.getMaxPathToUtilization().getValue();
        long liveSpaceUsedBytes = ColumnFamilyStore.getIfExists(keyspace, columnFamily).metric.liveDiskSpaceUsed.getCount();

        if (liveSpaceUsedBytes < THROTTLER_TABLE_SIZE_BYTES || diskUsage < 0.80d)
        {
            logger.trace("No disk pressure or table {} too small {} - using default compaction throughput {} with rate limit of {} for disk usage {}.",
                         SafeArg.of("ksCfPair", String.format("%s/%s", keyspace, columnFamily)),
                         SafeArg.of("liveSpaceUsedBytes", liveSpaceUsedBytes),
                         SafeArg.of("defaultCompactionThroughputMbPerSec", StorageService.instance.getCompactionThroughputMbPerSec()),
                         SafeArg.of("defaultRateLimit", defaultRateLimit),
                         SafeArg.of("diskUsage", diskUsage));
            return defaultRateLimit;
        }

        RateLimiter recommendedRateLimit = defaultRateLimit;
        if (diskUsage >= 0.90d)
        {
            recommendedRateLimit = RateLimiter.create(Math.min(defaultRateLimit.getRate() / 8, defaultRateLimit.getRate()));
        }
        else if (diskUsage >= 0.85d)
        {
            recommendedRateLimit = RateLimiter.create(Math.min(defaultRateLimit.getRate() / 4, defaultRateLimit.getRate()));
        }
        else if (diskUsage >= 0.80d)
        {
            recommendedRateLimit = RateLimiter.create(Math.min(defaultRateLimit.getRate() / 2, defaultRateLimit.getRate()));
        }

        logger.debug("Due to disk usage of {} and table size of {} for ks/cf {}, recommending throughput of {}MBs with rate limit of {}. " +
                     "Overwritten defaults were throughput {}MBs and rate limit {}.",
                     SafeArg.of("diskUsage", diskUsage),
                     SafeArg.of("liveSpaceUsedBytes", liveSpaceUsedBytes),
                     SafeArg.of("ksCfPair", String.format("%s/%s", keyspace, columnFamily)),
                     SafeArg.of("recommendedRateLimitMbs", recommendedRateLimit.getRate() / 1024/ 1024),
                     SafeArg.of("recommendedRateLimit", recommendedRateLimit),
                     SafeArg.of("defaultCompactionThroughput", StorageService.instance.getCompactionThroughputMbPerSec()),
                     SafeArg.of("defaultRateLimiter", CompactionManager.instance.getRateLimiter()));

        return recommendedRateLimit;
    }
}
