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

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

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
    private boolean successfulReadFromPersister = false;

    public CleanupStateTracker()
    {
        this.persister = new KeyspaceTableOpStatePersister(cleanupStateFileLocation());

        Optional<Map<KeyspaceTableKey, Instant>> maybeStateFromPersister = persister.readStateFromPersistentLocation();
        if (maybeStateFromPersister.isPresent())
            successfulReadFromPersister = true;

        this.state = new KeyspaceTableOpStateCache(maybeStateFromPersister.orElse(new HashMap<>()));
    }

    @VisibleForTesting
    Path cleanupStateFileLocation()
    {
        return Paths.get(DatabaseDescriptor.getPersistentSettingsLocation() + "/" + CLEANUP_STATE_FILE_NAME);
    }

    @VisibleForTesting
    CleanupStateTracker(KeyspaceTableOpStateCache state, KeyspaceTableOpStatePersister persister)
    {
        this.persister = persister;
        this.state = state;
        this.successfulReadFromPersister = true;
    }

    /** Creates table entry if it does not already exist */
    public synchronized void createCleanupEntryForTableIfNotExists(String keyspaceName, String columnFamily, Optional<Instant> cleanupTs)
        throws IllegalArgumentException
    {
        KeyspaceTableKey entryKey = KeyspaceTableKey.of(keyspaceName, columnFamily);
        if (!state.entryExists(entryKey))
            updateTsForEntry(entryKey, cleanupTs.orElse(MIN_TS));
    }

    /** Updates ts for table entry */
    public synchronized void recordSuccessfulCleanupForTable(String keyspaceName, String columnFamily)
        throws IllegalArgumentException
    {
        updateTsForEntry(KeyspaceTableKey.of(keyspaceName, columnFamily), Instant.now());
    }

    /** Returns min ts for this node, null if no entry exists. */
    public synchronized Instant getLastSuccessfulCleanupTsForNode()
    {
        updateCacheIfHasNotYetSuccessfullyReadFromPersister();
        return state.getMinimumTsOfAllEntries().orElse(MIN_TS);
    }

    @VisibleForTesting
    void updateTsForEntry(KeyspaceTableKey key, Instant value) throws IllegalArgumentException
    {
        Map<KeyspaceTableKey, Instant> updatedEntries = state.updateTsForEntry(key, value);
        updateCacheIfHasNotYetSuccessfullyReadFromPersister();
        if (!successfulReadFromPersister || !persister.updateStateInPersistentLocation(updatedEntries))
            log.warn("Failed to update persistant cleanup state, but cache has been updated. Will retry at next update.");
    }

    private void updateCacheIfHasNotYetSuccessfullyReadFromPersister()
    {
        if (successfulReadFromPersister)
            return;

        Optional<Map<KeyspaceTableKey, Instant>> maybeStateFromPersister = persister.readStateFromPersistentLocation();
        if (maybeStateFromPersister.isPresent())
        {
            // We always consider that the cache contains the most up-to-date entries
            maybeStateFromPersister.get().forEach((key, ts) -> {
                if (!state.entryExists(key))
                    state.updateTsForEntry(key, ts);
                 });
            successfulReadFromPersister = true;
        }
    }
}
