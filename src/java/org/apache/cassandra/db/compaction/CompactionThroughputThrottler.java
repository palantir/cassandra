package org.apache.cassandra.db.compaction;

import com.google.common.util.concurrent.RateLimiter;
import com.palantir.logsafe.SafeArg;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.service.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Backpressure mechanism to reduce compaction throughput as disk fills up
 */
public class CompactionThroughputThrottler
{
    private static final long THROTTLER_TABLE_SIZE_BYTES = Long.getLong("palantir_cassandra.throttler_table_size_gb", 10L) * 1024L * 1024L * 1024L;
    private static final Logger logger = LoggerFactory.getLogger(CompactionThroughputThrottler.class);

    public static RateLimiter getRateLimiter(String keyspace, String columnFamily)
    {
        RateLimiter recommendedCompactionThroughputMbPerSec = CompactionManager.instance.getRateLimiter();
        long liveSpaceUsedBytes = ColumnFamilyStore.getIfExists(keyspace, columnFamily).metric.liveDiskSpaceUsed.getCount();
        if (liveSpaceUsedBytes < THROTTLER_TABLE_SIZE_BYTES)
        {
            // table less than 10G and considered small, bypass throttle mechanism
            return recommendedCompactionThroughputMbPerSec;
        }

        int defaultCompactionThroughput = StorageService.instance.getCompactionThroughputMbPerSec();
        double diskUsage = Directories.getMaxPathToUtilization().getValue();

        if (diskUsage > 0.90d)
        {
            // 10 mb/s
            recommendedCompactionThroughputMbPerSec.setRate(10d);
        }
        else if (diskUsage > 0.85d)
        {
            // 20 mb/s
            recommendedCompactionThroughputMbPerSec.setRate(20d);
        }
        else if (diskUsage > 0.80d)
        {
            // 40 mb/s
            recommendedCompactionThroughputMbPerSec.setRate(40d);
        }
        else if (diskUsage >= 0.75d)
        {
            // 80 mb/s
            recommendedCompactionThroughputMbPerSec.setRate(80d);
        }

        logger.debug("Using recommended rate limit of {} due to disk usage of {} and default throughput {} for compaction of keyspace/table {}/{}",
                     SafeArg.of("recommendedCompactionThroughputMbPerSec", recommendedCompactionThroughputMbPerSec),
                     SafeArg.of("diskUsage", diskUsage),
                     SafeArg.of("defaultCompactionThroughput", defaultCompactionThroughput),
                     SafeArg.of("keyspace", keyspace),
                     SafeArg.of("table", columnFamily));

        return recommendedCompactionThroughputMbPerSec;
    }
}
