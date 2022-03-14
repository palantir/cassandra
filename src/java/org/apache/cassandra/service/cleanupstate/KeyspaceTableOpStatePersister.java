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

    private final File persistentFile;

    public KeyspaceTableOpStatePersister(String persistentFileLocation) throws IOException
    {
        File operationStateFile = new File(persistentFileLocation);
        try
        {
            operationStateFile.createNewFile();
            this.persistentFile = operationStateFile;
        }
        catch (IOException e)
        {
            log.error("Cannot retrieve or create state file.", operationStateFile.getAbsolutePath(), e);
            throw e;
        }
    }

    public Map<KeyspaceTableKey, Instant> readStateFromFile() throws IOException
    {
        if (persistentFile.length() == 0)
            return new HashMap<>();
        try
        {
            return convertMapTypeToKeyspaceTableKeyInstant(OBJECT_MAPPER.readValue(persistentFile, Map.class));
        }
        catch (Exception e)
        {
            log.error("Failed to retrieve state from file.", e);
            throw e;
        }
    }

    public void updateFileWithNewState(Map<KeyspaceTableKey, Instant> updatedEntries) throws IOException
    {
        try
        {
            OBJECT_MAPPER.writeValue(persistentFile, convertMapTypeToStringLong(updatedEntries));
        }
        catch (IOException e)
        {
            log.error("Failed to update state file.", e);
            throw e;
        }
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
