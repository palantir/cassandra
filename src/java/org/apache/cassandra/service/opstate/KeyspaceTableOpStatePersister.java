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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.ObjectMapper;

public class KeyspaceTableOpStatePersister
{
    private static final Logger log = LoggerFactory.getLogger(KeyspaceTableOpStatePersister.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private File persistentFile;
    private final Path persistentFileLocation;

    public KeyspaceTableOpStatePersister(Path persistentFileLocation)
    {
        this.persistentFileLocation = persistentFileLocation;
        this.persistentFile = getOrMaybeCreateStateFile();
    }

    public Optional<Map<KeyspaceTableKey, Instant>> readStateFromPersistentLocation()
    {
        File persistentStateFile = getOrMaybeCreateStateFile();
        if (persistentStateFile == null)
            return Optional.empty();

        try
        {
            return Optional.of(readStateFromFile(persistentStateFile));
        }
        catch (IOException e)
        {
            if (e instanceof JsonParseException)
            {
                log.warn("Persistent file corrupted, wiping content.");
                writeStateToFile(persistentStateFile, ImmutableMap.of());
            }
            return Optional.empty();
        }
    }

    public boolean updateStateInPersistentLocation(Map<KeyspaceTableKey, Instant> updatedEntries)
    {
        File persistentStateFile = getOrMaybeCreateStateFile();
        if (persistentStateFile == null)
            return false;

        return writeStateToFile(persistentStateFile, updatedEntries);
    }

    private File getOrMaybeCreateStateFile()
    {
        if (persistentFile != null)
            return persistentFile;

        File operationStateFile = persistentFileLocation.toFile();
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

    private Map<KeyspaceTableKey, Instant> readStateFromFile(File file) throws IOException
    {
        if(file.length() == 0)
            return new HashMap<>();
        try
        {
            return convertMapTypeToKeyspaceTableKeyInstant(OBJECT_MAPPER.readValue(file, Map.class));
        }
        catch (IOException e)
        {
            log.warn("Failed to read state from file.", file.getAbsolutePath(), e);
            throw e;
        }
    }

    private boolean writeStateToFile(File file, Map<KeyspaceTableKey, Instant> updatedEntries)
    {
        try
        {
            atomicWritetoFile(file, OBJECT_MAPPER.writeValueAsString(convertMapTypeToStringLong(updatedEntries)));
            return true;
        }
        catch (IOException e)
        {
            return false;
        }
    }

    private synchronized void atomicWritetoFile(File file, String content) throws IOException
    {
        String tmpFilePath = file.getAbsolutePath() + ".tmp";
        File tmpFile = new File(tmpFilePath);
        try
        {
            tmpFile.createNewFile();
            Files.write(tmpFile.toPath(), content.getBytes(), StandardOpenOption.TRUNCATE_EXISTING);
            Files.move(tmpFile.toPath(), file.toPath(),
                       StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        }
        catch (IOException e)
        {
            log.warn("Failed to write state to file.", file.getAbsolutePath(), e);
            throw e;
        }
        finally
        {
            Files.delete(tmpFile.toPath());
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
