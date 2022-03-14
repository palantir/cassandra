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

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.codehaus.jackson.map.ObjectMapper;

public class KeyspaceTableOpStatePersister
{
    private static final Logger log = LoggerFactory.getLogger(KeyspaceTableOpStatePersister.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private File persistentFile;
    private final String persistentFileLocation;

    public KeyspaceTableOpStatePersister(String persistentFileLocation)
    {
        this.persistentFileLocation = persistentFileLocation;
        this.persistentFile = getOrMaybeCreateStateFile();
    }

    public Map<KeyspaceTableKey, Instant> readStateFromFile()
    {
        File persistentStateFile = getOrMaybeCreateStateFile();
        if (persistentStateFile == null || persistentStateFile.length() == 0)
            return new HashMap<>();

        try
        {
            return convertMapTypeToKeyspaceTableKeyInstant(OBJECT_MAPPER.readValue(persistentStateFile, Map.class));
        }
        catch (Exception e)
        {
            log.warn("Failed to retrieve state from file.", persistentStateFile.getAbsolutePath(), e);
            return new HashMap<>();
        }
    }

    public boolean updateFileWithNewState(Map<KeyspaceTableKey, Instant> updatedEntries)
    {
        File persistentStateFile = getOrMaybeCreateStateFile();
        if (persistentStateFile == null)
            return false;

        try
        {
            OBJECT_MAPPER.writeValue(persistentStateFile, convertMapTypeToStringLong(updatedEntries));
        }
        catch (IOException e)
        {
            log.warn("Failed to update state file.", persistentStateFile.getAbsolutePath(), e);
            return false;
        }
        return true;
    }

    private File getOrMaybeCreateStateFile()
    {
        if (persistentFile != null)
            return persistentFile;

        File operationStateFile = new File(persistentFileLocation);
        try
        {
            operationStateFile.createNewFile();
        }
        catch (IOException e)
        {
            log.warn("Cannot retrieve or create state file.", operationStateFile.getAbsolutePath(), e);
            return null;
        }
        this.persistentFile = operationStateFile;
        return operationStateFile;
    }

    private static Map<KeyspaceTableKey, Instant> convertMapTypeToKeyspaceTableKeyInstant(Map<Object, Object> map)
    {
        Map<KeyspaceTableKey, Instant> typedMap = new HashMap<>();
        map.forEach((key, value) ->
                    typedMap.put(KeyspaceTableKey.parse(key.toString()), Instant.ofEpochMilli(Long.parseLong(value.toString()))));
        return typedMap;
    }

    private static Map<String, Long> convertMapTypeToStringLong(Map<KeyspaceTableKey, Instant> map)
    {
        Map<String, Long> typedMap = new HashMap<>();
        map.forEach((key, value) -> typedMap.put(key.toString(), value.toEpochMilli()));
        return typedMap;
    }

}
