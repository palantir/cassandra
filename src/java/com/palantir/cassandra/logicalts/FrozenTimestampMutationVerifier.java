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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.collect.Iterables;

import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.io.sstable.ColumnStats;

public class FrozenTimestampMutationVerifier implements MutationVerifier
{
    private final FrozenTimestampTrackerFactory frozenTimestampTrackerFactory;
    private final Map<String, FrozenTimestampTracker> namespaceToFrozenTimestampTracker;

    public FrozenTimestampMutationVerifier(FrozenTimestampTrackerFactory frozenTimestampTrackerFactory)
    {
        this.frozenTimestampTrackerFactory = frozenTimestampTrackerFactory;
        this.namespaceToFrozenTimestampTracker = new ConcurrentHashMap<>();
    }

    @Override
    public UncheckedAutoCloseable verifyMutations(Collection<? extends IMutation> mutations) throws IllegalLogicalTimestampException
    {
        if (mutations.size() == 1)
        {
            return verifyMutation(Iterables.getOnlyElement(mutations));
        }
        Map<String, Long> keyspaceToMaxWriteTimestamp = computeKeyspaceToMaxWriteTimestamp(mutations);
        List<UncheckedAutoCloseable> locks = acquireAllLocks(keyspaceToMaxWriteTimestamp);

        return () -> locks.forEach(UncheckedAutoCloseable::close);
    }

    @Override
    public UncheckedAutoCloseable verifyMutation(IMutation mutation) throws IllegalLogicalTimestampException
    {
        return acquireLock(mutation.getKeyspaceName(), maxTimestamp(mutation));
    }

    private List<UncheckedAutoCloseable> acquireAllLocks(Map<String, Long> keyspaceToMaxWriteTimestamp) throws IllegalLogicalTimestampException
    {
        List<UncheckedAutoCloseable> locks = new ArrayList<>();

        for (Map.Entry<String, Long> keyspaceAndMaxWriteTimestamp : keyspaceToMaxWriteTimestamp.entrySet())
        {
            String keyspaceName = keyspaceAndMaxWriteTimestamp.getKey();
            Long maxWriteTimestamp = keyspaceAndMaxWriteTimestamp.getValue();
            locks.add(acquireLock(keyspaceName, maxWriteTimestamp));
        }
        return locks;
    }

    private UncheckedAutoCloseable acquireLock(String keyspaceName, Long maxWriteTimestamp) throws IllegalLogicalTimestampException
    {
        return namespaceToFrozenTimestampTracker.computeIfAbsent(
                    keyspaceName,
                    ignored -> frozenTimestampTrackerFactory.create()).checkAndLockForMutation(maxWriteTimestamp);
    }

    private static Map<String, Long> computeKeyspaceToMaxWriteTimestamp(Collection<? extends IMutation> mutations)
    {
        Map<String, Long> keyspaceToMaxWriteTimestamp = new HashMap<>();

        for (IMutation mutation : mutations)
        {
            String keyspaceName = mutation.getKeyspaceName();
            long maxTimestamp = maxTimestamp(mutation);
            keyspaceToMaxWriteTimestamp.compute(
                keyspaceName,
                (ignored, current) -> current != null ? Long.max(current, maxTimestamp) : maxTimestamp);
        }
        return keyspaceToMaxWriteTimestamp;
    }

    private static long maxTimestamp(IMutation mutation)
    {
        ColumnStats.MaxLongTracker tracker = new ColumnStats.MaxLongTracker(Long.MIN_VALUE);
        for (ColumnFamily columnFamily : mutation.getColumnFamilies())
        {
            for (Cell cell : columnFamily)
            {
                tracker.update(cell.timestamp());
            }
        }
        return tracker.get();
    }
}
