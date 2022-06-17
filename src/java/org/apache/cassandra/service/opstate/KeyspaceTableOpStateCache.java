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

package org.apache.cassandra.service.opstate;


import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;

import org.apache.cassandra.config.Schema;

public class KeyspaceTableOpStateCache
{
    private final ConcurrentMap<KeyspaceTableKey, Instant> tableEntries;

    public KeyspaceTableOpStateCache(Map<KeyspaceTableKey, Instant> initialEntries)
    {
        this.tableEntries = new ConcurrentHashMap<>(initialEntries);
    }

    @VisibleForTesting
    Map<KeyspaceTableKey, Instant> getTableEntries()
    {
        return Collections.unmodifiableMap(tableEntries);
    }

    public boolean entryExists(KeyspaceTableKey entryKey)
    {
        return tableEntries.containsKey(entryKey);
    }

    public Map<KeyspaceTableKey, Instant> updateTsForEntry(KeyspaceTableKey entryKey, Instant value)
        throws IllegalArgumentException
    {
        if (tableEntries.containsKey(entryKey) && tableEntries.get(entryKey).compareTo(value) > 0)
            throw new IllegalArgumentException("Can only update cache entry with increasing timestamp");

        tableEntries.put(entryKey, value);
        return Collections.unmodifiableMap(tableEntries);
    }

    public Optional<Instant> getMinimumTsOfAllEntries()
    {
        if (tableEntries.isEmpty())
            return Optional.empty();

        // Remove potentially deleted keyspaces and tables from entries
        Set<KeyspaceTableKey> invalidEntries =
            Sets.difference(tableEntries.keySet(), Sets.intersection(tableEntries.keySet(), getValidKeyspaceTableEntries()));
        invalidEntries.forEach(tableEntries::remove);

        return Optional.of(Collections.min(tableEntries.values()));
    }


    @VisibleForTesting
    Set<KeyspaceTableKey> getValidKeyspaceTableEntries(){
        return Schema.instance.getKeyspaces().stream().map(keyspace ->
                                                           Schema.instance.getKeyspaceInstance(keyspace).getColumnFamilyStores()
                                                                          .stream()
                                                                          .map(cf -> KeyspaceTableKey.of(keyspace, cf.getColumnFamilyName()))

                                                                          .collect(Collectors.toSet())).flatMap(Set::stream)
                              .collect(Collectors.toSet());
    }
}
