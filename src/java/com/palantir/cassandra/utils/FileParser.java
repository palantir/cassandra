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

package com.palantir.cassandra.utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Optional;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class FileParser<T>
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final Path path;

    private final TypeReference<T> cls;

    public FileParser(Path path, TypeReference<T> cls)
    {
        this.path = path;
        this.cls = cls;
    }

    public Optional<T> read() throws IOException
    {
        File file = path.toFile();
        if (Files.isRegularFile(path) && file.length() > 0)
        {
            return Optional.of(OBJECT_MAPPER.readValue(file, cls));
        }
        else
        {
            return Optional.empty();
        }
    }

    public void write(T value) throws IOException
    {
        String content = OBJECT_MAPPER.writeValueAsString(value);

        String tmp = path.toFile().getAbsolutePath() + ".tmp";
        File tmpFile = new File(tmp);

        try
        {
            Files.write(tmpFile.toPath(), content.getBytes(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            Files.move(tmpFile.toPath(), path.toFile().toPath(), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        }
        finally
        {
            Files.deleteIfExists(tmpFile.toPath());
        }
    }

    public boolean createFileIfNotExists() throws IOException
    {
        if (Files.notExists(path))
        {
            Files.createFile(path);
            return true;
        }
        return false;
    }
}
