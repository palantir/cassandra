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
import java.time.Instant;
import java.util.AbstractMap;

import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class KeyspaceTableOpStatePersisterTest
{
    private static Path stateFilePath;
    private static Path tmpStateFilePath;

    @BeforeClass
    public static void before() throws IOException
    {
        Path directory = Files.createTempDirectory(OpStateTestConstants.TEST_DIRECTORY_NAME);
        stateFilePath = directory.resolve(OpStateTestConstants.TEST_STATE_FILE_NAME);
        tmpStateFilePath = directory.resolve(OpStateTestConstants.TEST_TMP_STATE_FILE_NAME);
    }

    @After
    public void afterEach()
    {
        stateFilePath.toFile().delete();
        tmpStateFilePath.toFile().delete();
    }

    @Test
    public void keyspaceTableOpStatePersisterSuccessfullyCreateFileWhenDoesNotExist()
    {
        new KeyspaceTableOpStatePersister(stateFilePath);
        assertThat(stateFilePath.toFile()).exists();
    }

    @Test
    public void keyspaceTableOpStatePersisterReturnsEmptyMapAtFileCreation()
    {
        KeyspaceTableOpStatePersister persister = new KeyspaceTableOpStatePersister(stateFilePath);
        assertThat(persister.readStateFromPersistentLocation().get()).isEmpty();
    }

    @Test
    public void keyspaceTableOpStatePersisterSuccessfullyUpdatesFile()
    {
        KeyspaceTableOpStatePersister persister = new KeyspaceTableOpStatePersister(stateFilePath);
        assertThat(persister.readStateFromPersistentLocation().get()).isEmpty();

        persister.updateStateInPersistentLocation(
            ImmutableMap.of(OpStateTestConstants.KEYSPACE_TABLE_KEY_1, Instant.ofEpochMilli(10L)));
        assertThat(persister.readStateFromPersistentLocation().get()).containsExactly(
            new AbstractMap.SimpleEntry<>(OpStateTestConstants.KEYSPACE_TABLE_KEY_1, Instant.ofEpochMilli(10L)));
    }

    @Test
    public void keyspaceTableOpStatePersisterUpdatesFileAtomically() throws IOException
    {
        KeyspaceTableOpStatePersister persister = new KeyspaceTableOpStatePersister(stateFilePath);
        assertThat(persister.readStateFromPersistentLocation().get()).isEmpty();
        tmpStateFilePath.toFile().createNewFile();
        tmpStateFilePath.toFile().setReadOnly();

        assertThat(persister.updateStateInPersistentLocation(
                ImmutableMap.of(OpStateTestConstants.KEYSPACE_TABLE_KEY_1, Instant.ofEpochMilli(10L)))).isFalse();
        assertThat(tmpStateFilePath.toFile().exists()).isFalse();
        assertThat(persister.readStateFromPersistentLocation().get()).isEmpty();
    }

    @Test
    public void keyspaceTableOpStatePersisterFixesFileWhenCorrupted() throws IOException
    {
        String corruptedFileContent = "{iamcorrupted";

        KeyspaceTableOpStatePersister persister = new KeyspaceTableOpStatePersister(stateFilePath);
        assertThat(persister.readStateFromPersistentLocation().get()).isEmpty();
        Files.write(stateFilePath, corruptedFileContent.getBytes());

        // First read fixes file but returns empty optional
        assertThat(persister.readStateFromPersistentLocation()).isEmpty();
        // Second read returns empty map
        assertThat(persister.readStateFromPersistentLocation().get()).isEmpty();
    }

    @Test
    public void keyspaceTableOpStatePersisterSuccessfullyRetrievesFileWhenExists() throws IOException
    {
        new KeyspaceTableOpStatePersister(stateFilePath);
        File testOpStateFile = stateFilePath.toFile();
        assertThat(testOpStateFile).exists();
        OpStateTestConstants.OBJECT_MAPPER.writeValue(
            testOpStateFile, ImmutableMap.of(OpStateTestConstants.KEYSPACE_TABLE_KEY_1.toString(), 10L));

        KeyspaceTableOpStatePersister persister = new KeyspaceTableOpStatePersister(stateFilePath);
        assertThat(persister.readStateFromPersistentLocation().get()).containsExactly(
            new AbstractMap.SimpleEntry<>(OpStateTestConstants.KEYSPACE_TABLE_KEY_1, Instant.ofEpochMilli(10L)));
    }
}
