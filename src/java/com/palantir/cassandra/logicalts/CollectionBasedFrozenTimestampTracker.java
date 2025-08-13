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

import java.util.NavigableSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;

public class CollectionBasedFrozenTimestampTracker implements FrozenTimestampTracker
{
    public static final FrozenTimestampTracker INSTANCE = new CollectionBasedFrozenTimestampTracker();
    private final NavigableSet<Long> activeReaders = new ConcurrentSkipListSet<>();
    private static final long UNINITIALIZED_TIMESTAMP = -1;

    private final AtomicLong frozenTimestamp = new AtomicLong(UNINITIALIZED_TIMESTAMP);

    @Override
    public void advanceFrozenTimestamp(long desiredTimestamp) throws InProgressMutationConflictException
    {
        if (timestampIsLocked(desiredTimestamp))
        {
            throw new InProgressMutationConflictException("There are in-progress mutations before the sweep timestamp. Sweep can not proceed.");
        }
        frozenTimestamp.updateAndGet(current -> Long.max(current, desiredTimestamp));
    }

    private boolean timestampIsLocked(long desiredTimestamp)
    {
        return activeReaders.floor(desiredTimestamp) != null;
    }

    @Override
    public UncheckedAutoCloseable checkAndLockForMutation(long mutationTimestamp) throws IllegalLogicalTimestampException
    {
        long frozenTimestamp = this.frozenTimestamp.get();
        if (frozenTimestamp == UNINITIALIZED_TIMESTAMP)
        {
            throw new IllegalLogicalTimestampException("Frozen timestamp has not been initialized");
        }
        if (mutationTimestamp < frozenTimestamp)
        {
            throw new IllegalLogicalTimestampException("Mutation start timestamp is before the frozen timestamp");
        }

        activeReaders.add(mutationTimestamp);

        if (mutationTimestamp < this.frozenTimestamp.get())
        {
            activeReaders.remove(mutationTimestamp);
            throw new IllegalLogicalTimestampException("Frozen timestamp was updated before we were able to lock our start timestamp");
        }

        // TODO: How can we ensure that this closable is ALWAYS executed? Or expire stale locks?
        return () -> activeReaders.remove(mutationTimestamp);
    }
}
