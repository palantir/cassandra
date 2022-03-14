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
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;

public class CleanupStateTracker
{
    private static final Logger log = LoggerFactory.getLogger(CleanupStateTracker.class);

    @VisibleForTesting
    static final Instant MIN_TS = Instant.EPOCH;
    private static final String CLEANUP_STATE_FILE_NAME = "node_cleanup_state.json";

    private final KeyspaceTableOpStateCache state;
    private final KeyspaceTableOpStatePersister persister;

    public CleanupStateTracker()
    {
        this.persister = new KeyspaceTableOpStatePersister(cleanupStateFileLocation());
        this.state = new KeyspaceTableOpStateCache(persister.readStateFromFile());
    }

    @VisibleForTesting
    String cleanupStateFileLocation()
    {
        return DatabaseDescriptor.getPersistentSettingsLocation() + "/" + CLEANUP_STATE_FILE_NAME;
    }

    @VisibleForTesting
    CleanupStateTracker(KeyspaceTableOpStateCache state, KeyspaceTableOpStatePersister persister)
    {
        this.persister = persister;
        this.state = state;
    }

    /** Creates table entry if it does not already exist */
    public synchronized void createCleanupEntryForTableIfNotExists(String keyspaceName, String columnFamily)
        throws IllegalArgumentException
    {
        KeyspaceTableKey entryKey = KeyspaceTableKey.of(keyspaceName, columnFamily);
        if (!state.entryExists(entryKey))
        {
            updateTsForEntry(entryKey, MIN_TS);
        }
    }

    /** Updates ts for table entry */
    public synchronized void recordSuccessfulCleanupForTable(String keyspaceName, String columnFamily)
        throws IllegalArgumentException
    {
        updateTsForEntry(KeyspaceTableKey.of(keyspaceName, columnFamily), Instant.now());
    }

    /** Returns min ts for this node, null if no entry exists. */
    public Instant getLastSuccessfulCleanupTsForNode()
    {
        Instant minTsFromState = state.getMinimumTsOfAllEntries();
        if (minTsFromState == null)
            return MIN_TS;
        return minTsFromState;
    }

    @VisibleForTesting
    void updateTsForEntry(KeyspaceTableKey key, Instant value) throws IllegalArgumentException
    {
        Map<KeyspaceTableKey, Instant> updatedEntries = state.updateTsForEntry(key, value);
        if (!persister.updateFileWithNewState(updatedEntries))
            log.warn("Failed to update persistant cleanup state, but cache has been updated. Will retry at next update.");
    }
}
