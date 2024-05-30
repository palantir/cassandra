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

package com.palantir.cassandra.concurrent;

import com.google.common.util.concurrent.Uninterruptibles;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.StampedLock;

public class SweepProgressStateStoreWithLocks
{
    private final Map<String, Long> lastSweptTimestampByShard = new ConcurrentHashMap<>();

    private final StampedLock noWriteTsStampedLock = new StampedLock();
    private final ReentrantReadWriteLock.WriteLock noWriteTsLock = new ReentrantReadWriteLock().writeLock();
    private volatile long noWriteTs = -1L;
    
    void updateLastSweptTimestampForShardWithLock(String shard, Long timestamp)
    {
        lastSweptTimestampByShard.put(shard, timestamp);
        long maxSweepTs = lastSweptTimestampByShard.values().stream().max(Long::compareTo).orElse(-1L);

        if (maxSweepTs > noWriteTs)
        {
            noWriteTsLock.lock();
            try
            {
                if (maxSweepTs > noWriteTs)
                {
                    noWriteTs = maxSweepTs;
                }
            }
            finally
            {
                noWriteTsLock.unlock();
            }
        }
    }

    void updateLastSweptTimestampForShardWithStampedLock(String shard, Long timestamp)
    {
        lastSweptTimestampByShard.put(shard, timestamp);
        long maxSweepTs = lastSweptTimestampByShard.values().stream().max(Long::compareTo).orElse(-1L);
        long readStamp = noWriteTsStampedLock.readLock();
        if (maxSweepTs > noWriteTs)
        {
            while (true)
            {
                long writeStamp = noWriteTsStampedLock.tryConvertToWriteLock(readStamp);
                if (writeStamp != 0)
                {
                    noWriteTs = maxSweepTs;
                    noWriteTsStampedLock.unlockWrite(writeStamp);
                    break;
                }
                Uninterruptibles.sleepUninterruptibly(Duration.ofSeconds(1));
            }
        }
        else
        {
            noWriteTsStampedLock.unlockRead(readStamp);
        }
    }


}
