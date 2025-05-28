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
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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

        CleanupStateTracker tracker = new CleanupStateTracker(state, persister, true);
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

        CleanupStateTracker tracker = spy(new CleanupStateTracker(state, persister, true));
        tracker.createCleanupEntryForTableIfNotExists(
            OpStateTestConstants.KEYSPACE1, OpStateTestConstants.TABLE1, Optional.empty());
        verify(tracker, times(0)).updateTsForEntry(any(), any());
    }

    @Test
    public void createCleanupEntryForTableIfNotExistsUsesMinTsIfTsNotProvided()
    {
        KeyspaceTableOpStatePersister persister = new KeyspaceTableOpStatePersister(stateFilePath);
        KeyspaceTableOpStateCache state = new KeyspaceTableOpStateCache(ImmutableMap.of());

        CleanupStateTracker tracker = spy(new CleanupStateTracker(state, persister, true));
        tracker.createCleanupEntryForTableIfNotExists(OpStateTestConstants.KEYSPACE1, OpStateTestConstants.TABLE1, Optional.empty());
        verify(tracker, times(1))
            .updateTsForEntry(eq(OpStateTestConstants.KEYSPACE_TABLE_KEY_1), eq(CleanupStateTracker.MIN_TS));
    }

    @Test
    public void createCleanupEntryForTablesUsesProvidedTs()
    {
        KeyspaceTableOpStatePersister persister = new KeyspaceTableOpStatePersister(stateFilePath);
        KeyspaceTableOpStateCache state = new KeyspaceTableOpStateCache(ImmutableMap.of());

        CleanupStateTracker tracker = spy(new CleanupStateTracker(state, persister, true));
        tracker.createCleanupEntryForTableIfNotExists(
            OpStateTestConstants.KEYSPACE1, OpStateTestConstants.TABLE1, Optional.of(OpStateTestConstants.INSTANT_10));
        verify(tracker, times(1))
            .updateTsForEntry(eq(OpStateTestConstants.KEYSPACE_TABLE_KEY_1), eq(OpStateTestConstants.INSTANT_10));
    }

    @Test
    public void recordSuccessfulCleanupForTableUpdatesEntry()
    {
        Instant instant1 = Instant.now().minusSeconds(30);
        KeyspaceTableOpStatePersister persister = new KeyspaceTableOpStatePersister(stateFilePath);
        KeyspaceTableOpStateCache state =
            new KeyspaceTableOpStateCache(ImmutableMap.of(OpStateTestConstants.KEYSPACE_TABLE_KEY_1, instant1));

        CleanupStateTracker tracker = spy(new CleanupStateTracker(state, persister, true));
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
        CleanupStateTracker tracker = new CleanupStateTracker(state, persister, true);
        assertThat(tracker.getLastSuccessfulCleanupTsForNode()).isEqualTo(CleanupStateTracker.MIN_TS);
    }

    @Test
    public void recordSuccessfulCleanupForTableDoesNotPersistAndLosesCache()
    {
        Instant instant1 = Instant.now().minusSeconds(30);

        KeyspaceTableOpStateCache state = spy(new KeyspaceTableOpStateCache(ImmutableMap.of()));
        doReturn(OpStateTestConstants.KEYSPACE_TABLE_VALID_ENTRIES).when(state).getValidKeyspaceTableEntries();

        KeyspaceTableOpStatePersister persister = spy(new KeyspaceTableOpStatePersister(stateFilePath));
        // todo, wonky mock
        doReturn(false).when(persister).updateStateInPersistentLocation(
            argThat(argument -> {
                if (argument instanceof Map) {
                    Map<?, ?> map = (Map<?, ?>) argument;
                    return map.containsKey(OpStateTestConstants.KEYSPACE_TABLE_KEY_2) &&
                           map.get(OpStateTestConstants.KEYSPACE_TABLE_KEY_2).equals(instant1);
                }
                return false;
            })
        );

        CleanupStateTracker tracker = new CleanupStateTracker(state, persister, true);
        tracker.createCleanupEntryForTableIfNotExists(OpStateTestConstants.KEYSPACE1, OpStateTestConstants.TABLE1, Optional.of(instant1));
        tracker.createCleanupEntryForTableIfNotExists(OpStateTestConstants.KEYSPACE2, OpStateTestConstants.TABLE2, Optional.of(instant1));
        assertThat(tracker.getLastSuccessfulCleanupTsForNode()).isEqualTo(instant1);

        tracker = new CleanupStateTracker(new KeyspaceTableOpStateCache(ImmutableMap.of()), persister, false);
        assertThat(tracker.getLastSuccessfulCleanupTsForNode()).isEqualTo(CleanupStateTracker.MIN_TS);
    }
}
