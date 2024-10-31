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

package com.palantir.cassandra.logicalts;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public final class LockingFrozenTimestampTracker implements FrozenTimestampTracker
{
    private static final long UNINITIALIZED_TIMESTAMP = -1;

    private final AtomicLong frozenTimestamp = new AtomicLong(UNINITIALIZED_TIMESTAMP);

    private final ConcurrentMap<Long, ReadWriteLock> startTimestampLocks = new ConcurrentHashMap<>();

    @Override
    public void advanceFrozenTimestamp(long desiredTimestamp)
    {
        TreeMap<Long, ReadWriteLock> sortedLocks = new TreeMap<>();
        for (Map.Entry<Long, ReadWriteLock> entry : startTimestampLocks.entrySet())
        {
            if (entry.getKey() < desiredTimestamp)
            {
                sortedLocks.put(entry.getKey(), entry.getValue());
            }
        }
        try
        {
            for (ReadWriteLock lock : sortedLocks.values())
            {
                lock.writeLock().lockInterruptibly();
            }
            frozenTimestamp.updateAndGet(current -> Long.max(current, desiredTimestamp));
        }
        catch (InterruptedException ignored)
        {
        }
        finally
        {
            for (ReadWriteLock lock : sortedLocks.values())
            {
                if (lock.writeLock().tryLock())
                {
                    lock.writeLock().unlock();
                }
            }
        }
    }

    @Override
    public UncheckedAutoCloseable checkAndLockForMutation(long mutationTimestamp) throws IllegalLogicalTimestampException
    {
        long frozenTimestamp = this.frozenTimestamp.get();
        if (mutationTimestamp < frozenTimestamp || frozenTimestamp == UNINITIALIZED_TIMESTAMP)
        {
            throw new IllegalLogicalTimestampException("Cannot read data with a start timestamp less than the frozen timestamp");
        }
        ReadWriteLock lock = startTimestampLocks.computeIfAbsent(mutationTimestamp, ignored -> new ReentrantReadWriteLock());
        lock.readLock().lock();

        return () -> {
            startTimestampLocks.remove(mutationTimestamp);
            if (lock.readLock().tryLock())
            {
                lock.readLock().unlock();
            }
        };
    }
}
