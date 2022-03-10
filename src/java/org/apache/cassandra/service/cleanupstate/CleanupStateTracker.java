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

import java.io.IOException;
import java.time.Instant;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.DatabaseDescriptor;

public class CleanupStateTracker
{
    @VisibleForTesting
    static final Instant MIN_TS = Instant.EPOCH;
    private static final String CLEANUP_STATE_FILE_NAME = "node_cleanup_state.json";

    private final CleanupState state;
    private final CleanupStatePersister persister;

    public CleanupStateTracker() throws IOException
    {
        this.persister = new CleanupStatePersister(cleanupStateFileLocation());
        this.state = new CleanupState(persister.readCleanupStateFromFile());
    }

    @VisibleForTesting
    String cleanupStateFileLocation()
    {
        return DatabaseDescriptor.getPersistentSettingsLocation() + "/" + CLEANUP_STATE_FILE_NAME;
    }

    @VisibleForTesting
    CleanupStateTracker(CleanupState state, CleanupStatePersister persister)
    {
        this.persister = persister;
        this.state = state;
    }

    /** Creates table entry if it does not already exist */
    public synchronized void createCleanupEntryForTableIfNotExists(String keyspaceName, String columnFamily)
        throws IllegalArgumentException, IOException
    {
        String entryKey = convertKeyspaceTableToKey(keyspaceName, columnFamily);
        if (!state.entryExists(entryKey))
        {
            updateTsForEntry(entryKey, MIN_TS.toEpochMilli());
        }
    }

    /** Updates ts for table entry */
    public synchronized void recordSuccessfulCleanupForTable(String keyspaceName, String columnFamily)
        throws IllegalArgumentException, IOException
    {
        updateTsForEntry(convertKeyspaceTableToKey(keyspaceName, columnFamily), Instant.now().toEpochMilli());
    }

    /** Returns min ts for this node, null if no entry exists. */
    public Instant getLastSuccessfulCleanupTsForNode()
    {
        Long minTsFromState = state.getMinimumTsOfAllEntries();
        return minTsFromState == null? MIN_TS: Instant.ofEpochMilli(minTsFromState);
    }

    @VisibleForTesting
    void updateTsForEntry(String key, Long value) throws IllegalArgumentException, IOException
    {
        Map<String, Long> updatedEntries = state.updateTsForEntry(key, value);
        persister.updateFileWithNewState(updatedEntries);
    }

    private String convertKeyspaceTableToKey(String keyspaceName, String columnFamily)
    {
        return keyspaceName + ":" + columnFamily;
    }
}
