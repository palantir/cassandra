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

import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class NamespacedSweepProgressStateStore
{
    private final LongAccumulator noWriteTs = new LongAccumulator(Long::max, -1);
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public void updateLastSweptTimestamp(long timestamp)
    {
        lock.writeLock().lock();
        try
        {
            noWriteTs.accumulate(timestamp);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    public Lock readLock()
    {
        return lock.readLock();
    }

    /**
     * @apiNote You MUST hold the read lock while calling this method.
     */
    public long getNoWriteTs()
    {
        return noWriteTs.get();
    }
}
