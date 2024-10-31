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

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.IMutation;

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
        Map<String, Long> keyspaceToMaxWriteTimestamp = computeKeyspaceToMaxWriteTimestamp(mutations);
        List<UncheckedAutoCloseable> locks = acquireAllLocks(keyspaceToMaxWriteTimestamp);

        return () -> locks.forEach(UncheckedAutoCloseable::close);
    }

    private List<UncheckedAutoCloseable> acquireAllLocks(Map<String, Long> keyspaceToMaxWriteTimestamp) throws IllegalLogicalTimestampException
    {
        List<UncheckedAutoCloseable> locks = new ArrayList<>();

        for (Map.Entry<String, Long> keyspaceAndMaxWriteTimestamp : keyspaceToMaxWriteTimestamp.entrySet())
        {
            String keyspaceName = keyspaceAndMaxWriteTimestamp.getKey();
            Long maxWriteTimestamp = keyspaceAndMaxWriteTimestamp.getValue();
            locks.add(namespaceToFrozenTimestampTracker.computeIfAbsent(
                keyspaceName,
                ignored -> frozenTimestampTrackerFactory.create()).checkAndLockForMutation(maxWriteTimestamp));
        }
        return locks;
    }

    private static Map<String, Long> computeKeyspaceToMaxWriteTimestamp(Collection<? extends IMutation> mutations)
    {
        Map<String, Long> keyspaceToMaxWriteTimestamp = new HashMap<>();

        for (IMutation mutation : mutations)
        {
            for (ColumnFamily columnFamily : mutation.getColumnFamilies())
            {
                // TODO(rhuffman): getColumnStats() is expensive. Replace this
                long maxTimestamp = columnFamily.getColumnStats().maxTimestamp;

                keyspaceToMaxWriteTimestamp.compute(
                    mutation.getKeyspaceName(),
                    (ignored, current) -> current != null ? Long.max(current, maxTimestamp) : maxTimestamp);
            }
        }
        return keyspaceToMaxWriteTimestamp;
    }
}
