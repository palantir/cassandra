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
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class CleanupStateTrackerTest
{
    private static Path stateFilePath;

    @BeforeClass
    public static void before() throws IOException
    {
        Path directory = Files.createTempDirectory(OpStateTestConstants.TEST_DIRECTORY_NAME);
        stateFilePath = directory.resolve(OpStateTestConstants.TEST_STATE_FILE_NAME);
    }

    @After
    public void afterEach()
    {
        stateFilePath.toFile().delete();
    }

    @Test
    public void updateTsForEntryUpdatesBothStateAndPersistent()
    {
        KeyspaceTableOpStatePersister persister = spy(new KeyspaceTableOpStatePersister(stateFilePath));
        KeyspaceTableOpStateCache state = spy(new KeyspaceTableOpStateCache(ImmutableMap.of()));

        CleanupStateTracker tracker = new CleanupStateTracker(state, persister);
        tracker.updateTsForEntry(OpStateTestConstants.KEYSPACE_TABLE_KEY_1, Instant.ofEpochMilli(10L));
        verify(state, times(1))
            .updateTsForEntry(eq(OpStateTestConstants.KEYSPACE_TABLE_KEY_1), eq(Instant.ofEpochMilli(10L)));
        verify(persister, times(1))
            .updateStateInPersistentLocation(eq(ImmutableMap.of(OpStateTestConstants.KEYSPACE_TABLE_KEY_1, Instant.ofEpochMilli(10L))));
    }

    @Test
    public void createCleanupEntryForTableIfNotExistsDoesNothingIfEntryExists()
    {
        KeyspaceTableOpStatePersister persister = new KeyspaceTableOpStatePersister(stateFilePath);
        KeyspaceTableOpStateCache state =
            new KeyspaceTableOpStateCache(ImmutableMap.of(OpStateTestConstants.KEYSPACE_TABLE_KEY_1, Instant.ofEpochMilli(20L)));

        CleanupStateTracker tracker = spy(new CleanupStateTracker(state, persister));
        tracker.createCleanupEntryForTableIfNotExists(
            OpStateTestConstants.KEYSPACE1, OpStateTestConstants.TABLE1, Optional.empty());
        verify(tracker, times(0)).updateTsForEntry(any(), any());
    }

    @Test
    public void createCleanupEntryForTableSucceedsIfEntryDoesNotExist()
    {
        KeyspaceTableOpStatePersister persister = new KeyspaceTableOpStatePersister(stateFilePath);
        KeyspaceTableOpStateCache state = new KeyspaceTableOpStateCache(ImmutableMap.of());

        CleanupStateTracker tracker = spy(new CleanupStateTracker(state, persister));
        tracker.createCleanupEntryForTableIfNotExists(OpStateTestConstants.KEYSPACE1, OpStateTestConstants.TABLE1, Optional.empty());
        verify(tracker, times(1))
            .updateTsForEntry(eq(OpStateTestConstants.KEYSPACE_TABLE_KEY_1), eq(CleanupStateTracker.MIN_TS));
    }

    @Test
    public void recordSuccessfulCleanupForTableUpdatesEntry()
    {
        Instant instant1 = Instant.now().minusSeconds(30);
        KeyspaceTableOpStatePersister persister = new KeyspaceTableOpStatePersister(stateFilePath);
        KeyspaceTableOpStateCache state =
            new KeyspaceTableOpStateCache(ImmutableMap.of(OpStateTestConstants.KEYSPACE_TABLE_KEY_1, instant1));

        CleanupStateTracker tracker = spy(new CleanupStateTracker(state, persister));
        Map<KeyspaceTableKey, Instant> cacheEntries = state.getTableEntries();
        assertThat(cacheEntries.get(KeyspaceTableKey.of(OpStateTestConstants.KEYSPACE1, OpStateTestConstants.TABLE1)))
            .isEqualTo(instant1);

        tracker.recordSuccessfulCleanupForTable(OpStateTestConstants.KEYSPACE1, OpStateTestConstants.TABLE1);
        assertThat(state.getTableEntries()
                        .get(OpStateTestConstants.KEYSPACE_TABLE_KEY_1).compareTo(instant1))
            .isGreaterThan(0);
    }

    @Test
    public void getLastSuccessfulCleanupTsForNodeReturnsMinTsIfNoEntriesExist()
    {
        KeyspaceTableOpStatePersister persister = new KeyspaceTableOpStatePersister(stateFilePath);
        KeyspaceTableOpStateCache state = new KeyspaceTableOpStateCache(ImmutableMap.of());
        CleanupStateTracker tracker = new CleanupStateTracker(state, persister);
        assertThat(tracker.getLastSuccessfulCleanupTsForNode()).isEqualTo(CleanupStateTracker.MIN_TS);
    }
}
