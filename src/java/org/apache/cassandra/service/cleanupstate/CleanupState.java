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

package org.apache.cassandra.service.cleanupstate;


import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.google.common.annotations.VisibleForTesting;

public class CleanupState
{
    private final ConcurrentMap<String, Long> tableEntries;

    public CleanupState(Map<String, Long> initialEntries)
    {
        this.tableEntries = new ConcurrentHashMap<>(initialEntries);
    }

    @VisibleForTesting
    ConcurrentMap<String, Long> getTableEntries()
    {
        return tableEntries;
    }

    public boolean entryExists(String entryKey)
    {
        return tableEntries.containsKey(entryKey);
    }

    public Map<String, Long> updateTsForEntry(String entryKey, Long value)
        throws IllegalArgumentException
    {
        if (tableEntries.containsKey(entryKey) && tableEntries.get(entryKey).compareTo(value) > 0)
            throw new IllegalArgumentException("Can only update cleanup state entry with increasing timestamp");

        tableEntries.put(entryKey, value);
        return tableEntries;
    }

    public Long getMinimumTsOfAllEntries()
    {
        if (tableEntries.isEmpty())
            return null;

        return Collections.min(tableEntries.values());
    }
}
