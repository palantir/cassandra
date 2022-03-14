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
import java.util.AbstractMap;

import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class KeyspaceTableOpStatePersisterTest
{

    @After
    public void afterEach()
    {
        new File(CleanupStateTestConstants.TEST_CLEANUP_STATE_FILE_LOCATION).delete();
    }

    @Test
    public void operationStatePersisterSuccessfullyCreateFileWhenDoesNotExist()
    {
        new KeyspaceTableOpStatePersister(CleanupStateTestConstants.TEST_CLEANUP_STATE_FILE_LOCATION);
        assertThat(new File(CleanupStateTestConstants.TEST_CLEANUP_STATE_FILE_LOCATION)).exists();
    }

    @Test
    public void operationStatePersisterReturnsEmptyMapAtFileCreation()
    {
        KeyspaceTableOpStatePersister persister =
            new KeyspaceTableOpStatePersister(CleanupStateTestConstants.TEST_CLEANUP_STATE_FILE_LOCATION);
        assertThat(persister.readStateFromFile()).isEmpty();
    }

    @Test
    public void operationStatePersisterSuccessfullyUpdatesFile()
    {
        KeyspaceTableOpStatePersister persister =
            new KeyspaceTableOpStatePersister(CleanupStateTestConstants.TEST_CLEANUP_STATE_FILE_LOCATION);
        assertThat(persister.readStateFromFile()).isEmpty();

        persister.updateFileWithNewState(
            ImmutableMap.of(CleanupStateTestConstants.KEYSPACE_TABLE_KEY_1, Instant.ofEpochMilli(10L)));
        assertThat(persister.readStateFromFile()).containsExactly(
            new AbstractMap.SimpleEntry<>(CleanupStateTestConstants.KEYSPACE_TABLE_KEY_1, Instant.ofEpochMilli(10L)));
    }

    @Test
    public void cleanupStatePersisterSuccessfullyRetrievesFileWhenExists() throws IOException
    {
        new KeyspaceTableOpStatePersister(CleanupStateTestConstants.TEST_CLEANUP_STATE_FILE_LOCATION);
        File testCleanupStateFile = new File(CleanupStateTestConstants.TEST_CLEANUP_STATE_FILE_LOCATION);
        assertThat(testCleanupStateFile).exists();
        CleanupStateTestConstants.OBJECT_MAPPER.writeValue(
            testCleanupStateFile, ImmutableMap.of(CleanupStateTestConstants.KEYSPACE_TABLE_KEY_1.toString(), 10L));

        KeyspaceTableOpStatePersister persister =
            new KeyspaceTableOpStatePersister(CleanupStateTestConstants.TEST_CLEANUP_STATE_FILE_LOCATION);
        assertThat(persister.readStateFromFile()).containsExactly(
            new AbstractMap.SimpleEntry<>(CleanupStateTestConstants.KEYSPACE_TABLE_KEY_1, Instant.ofEpochMilli(10L)));
    }
}
