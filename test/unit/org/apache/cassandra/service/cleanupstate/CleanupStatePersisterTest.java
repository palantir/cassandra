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
import java.util.AbstractMap;

import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class CleanupStatePersisterTest
{

    @After
    public void afterEach(){
        new File(CleanupStateTestConstants.TEST_CLEANUP_STATE_FILE_LOCATION).delete();
    }

    @Test
    public void cleanupStatePersisterSuccessfullyCreateFileWhenDoesNotExist() throws IOException
    {
        new CleanupStatePersister(CleanupStateTestConstants.TEST_CLEANUP_STATE_FILE_LOCATION);
        assertThat(new File(CleanupStateTestConstants.TEST_CLEANUP_STATE_FILE_LOCATION)).exists();
    }

    @Test
    public void cleanupStatePersisterReturnsEmptyMapAtFileCreation() throws IOException
    {
        CleanupStatePersister persister = new CleanupStatePersister(CleanupStateTestConstants.TEST_CLEANUP_STATE_FILE_LOCATION);
        assertThat(persister.readCleanupStateFromFile()).isEmpty();
    }

    @Test
    public void cleanupStatePersisterSuccessfullyUpdatesFile() throws IOException
    {
        CleanupStatePersister persister = new CleanupStatePersister(CleanupStateTestConstants.TEST_CLEANUP_STATE_FILE_LOCATION);
        assertThat(persister.readCleanupStateFromFile()).isEmpty();

        persister.updateFileWithNewState(ImmutableMap.of(CleanupStateTestConstants.KEYSPACE1_TABLE1_KEY, 10L));
        assertThat(persister.readCleanupStateFromFile())
            .containsExactly(new AbstractMap.SimpleEntry<>(CleanupStateTestConstants.KEYSPACE1_TABLE1_KEY, 10L));
    }

    @Test
    public void cleanupStatePersisterSuccessfullyRetrievesFileWhenExists() throws IOException
    {
        new CleanupStatePersister(CleanupStateTestConstants.TEST_CLEANUP_STATE_FILE_LOCATION);
        File testCleanupStateFile = new File(CleanupStateTestConstants.TEST_CLEANUP_STATE_FILE_LOCATION);
        assertThat(testCleanupStateFile).exists();
        CleanupStateTestConstants.OBJECT_MAPPER.writeValue(testCleanupStateFile, ImmutableMap.of(CleanupStateTestConstants.KEYSPACE1_TABLE1_KEY, 10L));

        CleanupStatePersister persister = new CleanupStatePersister(CleanupStateTestConstants.TEST_CLEANUP_STATE_FILE_LOCATION);
        assertThat(persister.readCleanupStateFromFile()).containsExactly(new AbstractMap.SimpleEntry<>(CleanupStateTestConstants.KEYSPACE1_TABLE1_KEY, 10L));
    }
}
