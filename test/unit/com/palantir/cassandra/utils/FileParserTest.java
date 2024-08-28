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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.core.type.TypeReference;

import static org.assertj.core.api.Assertions.assertThat;

public final class FileParserTest
{
    private static final String TEST_DIRECTORY_NAME = "test-dir";

    private static final String FILE_NAME = "data.json";

    private static final String TMP_FILE_NAME = "data.json.tmp";

    private static Path file;

    private static Path tmpFile;

    private static final Map<String, String> DATA = ImmutableMap.of("k1", "v1", "k2", "v2");

    private static FileParser<Map<String, String>> parser;

    @BeforeClass
    public static void beforeClass() throws IOException
    {
        Path directory = Files.createTempDirectory(TEST_DIRECTORY_NAME);
        file = directory.resolve(FILE_NAME);
        tmpFile = directory.resolve(TMP_FILE_NAME);
        parser = new FileParser<>(file, new TypeReference<Map<String, String>>()
        {
        });
    }

    @Before
    public void before() throws IOException
    {
        Files.deleteIfExists(file);
        Files.deleteIfExists(tmpFile);
    }

    @Test
    public void read_noFileReturnsOptionalEmpty() throws IOException
    {
        assertThat(parser.read()).isEmpty();
    }

    @Test
    public void read_pathIsDirectoryReturnsOptionalEmpty() throws IOException
    {
        Files.createDirectories(file);
        assertThat(parser.read()).isEmpty();
    }

    @Test
    public void read_emptyFileReturnsOptionalEmpty() throws IOException
    {
        assertThat(parser.create()).isTrue();
        assertThat(parser.read()).isEmpty();
    }

    @Test
    public void write_successfullyWriteToFile() throws IOException
    {
        parser.write(DATA);
        assertThat(parser.read()).isPresent().hasValue(DATA);
    }

    @Test
    public void create_returnsTrueIfNotExistElseFalse() throws IOException
    {
        assertThat(parser.create()).isTrue();
        assertThat(parser.create()).isFalse();
    }
}
