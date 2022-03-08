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

package org.apache.cassandra.service;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.google.common.annotations.VisibleForTesting;

public class CleanupStateTracker
{
    private static final Logger log = LoggerFactory.getLogger(CleanupStateTracker.class);

    @VisibleForTesting
    static final String CLEANUP_STATE_FILE_NAME = "node_cleanup_state.json";
    private static final Instant MIN_TS = Instant.parse("2022-01-01T00:00:00Z");

    private final CleanupState cleanupStateCache;

    public CleanupStateTracker() throws IOException
    {
        this.cleanupStateCache = new CleanupState(createOrRetrieveCleanupStateFile());
    }

    @VisibleForTesting
    File createOrRetrieveCleanupStateFile() throws IOException
    {
        File cleanupStateFile = new File(cleanupStateFileLocation());
        try
        {
            cleanupStateFile.createNewFile();
            return cleanupStateFile;
        }
        catch (IOException e)
        {
            log.warn("Cannot retrieve or create node cleanup state file.", cleanupStateFile.getAbsolutePath(), e);
            throw e;
        }
    }

    @VisibleForTesting
    String cleanupStateFileLocation(){
        return DatabaseDescriptor.getPersistentSettingsLocation() + CLEANUP_STATE_FILE_NAME;
    }

    /** Creates table entry if it does not already exist */
    public synchronized void createCleanupEntryForTableIfNotExists(String keyspaceName, String columnFamily)
        throws IOException
    {
        if (!cleanupStateCache.entryExists(keyspaceName, columnFamily))
            cleanupStateCache.updateTsForEntry(keyspaceName, columnFamily, MIN_TS);
    }

    /** Updates ts for table entry */
    public synchronized void recordSuccessfulCleanupForTable(String keyspaceName, String columnFamily)
        throws IOException
    {
        cleanupStateCache.updateTsForEntry(keyspaceName, columnFamily,Instant.now());
    }

    /** Returns min ts for this node, null if no entry exists. */
    public Instant getLastSuccessfulCleanupTsForNode()
    {
        return cleanupStateCache.getMinimumTsOfAllEntries();
    }

    @VisibleForTesting
    static class CleanupState
    {
        private final ConcurrentMap<String, Long> tableEntries;
        private final File persistentCleanupStateFile;
        private final ObjectMapper objectMapper = new ObjectMapper();

        public CleanupState(File cleanupStateFile) throws IOException
        {
            this.persistentCleanupStateFile = cleanupStateFile;
            this.tableEntries = retrieveCleanupStateFromFile();
        }

        @VisibleForTesting
        ConcurrentMap<String, Long> getTableEntries()
        {
            return tableEntries;
        }

        private ConcurrentMap<String, Long> retrieveCleanupStateFromFile() throws IOException
        {
            if (persistentCleanupStateFile.length() == 0)
            {
               return new ConcurrentHashMap<>();
            }
            try
            {
                return convertObjectMapTypetoStringLong(objectMapper.readValue(persistentCleanupStateFile, Map.class));
            }
            catch (Exception e)
            {
                log.error("Failed to retrieve state from cleanup state file.", e);
                throw e;
            }
        }

        public boolean entryExists(String keyspaceName, String columnFamily)
        {
            String entryKey = keyspaceName + ":" + columnFamily;
            return tableEntries.containsKey(entryKey);
        }

        public synchronized void updateTsForEntry(String keyspaceName, String columnFamily, Instant value)
            throws IOException, IllegalArgumentException
        {
            String entryKey = keyspaceName + ":" + columnFamily;

            if (tableEntries.containsKey(entryKey) && tableEntries.get(entryKey).compareTo(value.toEpochMilli()) > 0)
                throw new IllegalArgumentException("Can only update cleanup state entry with increasing timestamp");

            tableEntries.put(entryKey, value.toEpochMilli());
            try
            {
                objectMapper.writeValue(persistentCleanupStateFile, tableEntries);
            }
            catch (Exception e)
            {
                log.error("Failed to update cleanup state file.", e);
                throw e;
            }
        }

        public Instant getMinimumTsOfAllEntries()
        {
            if (tableEntries.isEmpty())
                return null;

            return Instant.ofEpochMilli(Collections.min(tableEntries.values()));
        }

        @VisibleForTesting
        ConcurrentMap<String, Long> convertObjectMapTypetoStringLong(Map<Object, Object> map)
        {
            ConcurrentMap<String, Long> typedMap = new ConcurrentHashMap<>();
            map.forEach((key, value) -> typedMap.put(key.toString(), Long.parseLong(value.toString())));
            return typedMap;
        }
    }
}
