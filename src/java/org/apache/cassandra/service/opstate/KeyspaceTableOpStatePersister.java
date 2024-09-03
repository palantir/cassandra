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

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.palantir.cassandra.utils.FileParser;

public class KeyspaceTableOpStatePersister
{
    private static final Logger log = LoggerFactory.getLogger(KeyspaceTableOpStatePersister.class);

    private final Path persistentFileLocation;

    private final FileParser<Map<String, Long>> parser;

    public KeyspaceTableOpStatePersister(Path persistentFileLocation)
    {
        this.persistentFileLocation = persistentFileLocation;
        this.parser = new FileParser<>(persistentFileLocation, new TypeReference<Map<String, Long>>()
        {
        });
        init();
    }

    public Optional<Map<KeyspaceTableKey, Instant>> readStateFromPersistentLocation()
    {
        try
        {
            parser.createFileIfNotExists();
            return Optional.of(parser.read().map(KeyspaceTableOpStatePersister::convertMapTypeToKeyspaceTableKeyInstant)
                                     .orElse(ImmutableMap.of()));
        }
        catch (IOException e)
        {
            logIoException("Failed to read state from file.", persistentFileLocation.toFile().getAbsolutePath(), e);
            if (e instanceof JsonParseException)
            {
                log.warn("Persistent file corrupted, wiping content.");
                writeStateToFile(ImmutableMap.of());
            }
            return Optional.empty();
        }
    }

    public boolean updateStateInPersistentLocation(Map<KeyspaceTableKey, Instant> updatedEntries)
    {
        try
        {
            parser.createFileIfNotExists();
            return writeStateToFile(updatedEntries);
        }
        catch (IOException e)
        {
            logIoException("Cannot retrieve or create state file.", persistentFileLocation.toFile().getAbsolutePath(), e);
            return false;
        }
    }

    private boolean writeStateToFile(Map<KeyspaceTableKey, Instant> updatedEntries)
    {
        try
        {
            parser.write(convertMapTypeToStringLong(updatedEntries));
            return true;
        }
        catch (IOException e)
        {
            return false;
        }
    }

    private void init()
    {
        try
        {
            parser.createFileIfNotExists();
        }
        catch (IOException e)
        {
            logIoException("Failed to create state file.", persistentFileLocation.toFile().getAbsolutePath(), e);
        }
    }

    private void logIoException(String message, String path, IOException e)
    {
        log.warn(message, path, e);
    }

    private static Map<KeyspaceTableKey, Instant> convertMapTypeToKeyspaceTableKeyInstant(Map<String, Long> map)
    {
        Map<KeyspaceTableKey, Instant> typedMap = new HashMap<>();
        map.forEach((key, value) -> typedMap.put(KeyspaceTableKey.parse(key), Instant.ofEpochMilli(value)));
        return typedMap;
    }

    private static Map<String, Long> convertMapTypeToStringLong(Map<KeyspaceTableKey, Instant> map)
    {
        Map<String, Long> typedMap = new HashMap<>();
        map.forEach((key, value) -> typedMap.put(key.toString(), value.toEpochMilli()));
        return typedMap;
    }
}
