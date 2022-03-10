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

import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class CleanupStateTrackerTest
{

    @After
    public void afterEach(){
        new File(CleanupStateTestConstants.TEST_CLEANUP_STATE_FILE_LOCATION).delete();
    }

    @Test
    public void updateTsForEntryUpdatesBothStateAndPersistent() throws IOException
    {
        CleanupStatePersister persister =
            spy(new CleanupStatePersister(CleanupStateTestConstants.TEST_CLEANUP_STATE_FILE_LOCATION));
        CleanupState state = spy(new CleanupState(ImmutableMap.of()));

        CleanupStateTracker tracker = new CleanupStateTracker(state, persister);
        tracker.updateTsForEntry(CleanupStateTestConstants.KEYSPACE1_TABLE1_KEY, 10L);
        verify(state, times(1))
            .updateTsForEntry(eq(CleanupStateTestConstants.KEYSPACE1_TABLE1_KEY), eq(10L));
        verify(persister, times(1))
            .updateFileWithNewState(eq(ImmutableMap.of(CleanupStateTestConstants.KEYSPACE1_TABLE1_KEY, 10L)));
    }

    @Test
    public void createCleanupEntryForTableIfNotExistsDoesNothingIfEntryExists() throws IOException
    {
        CleanupStatePersister persister =
            new CleanupStatePersister(CleanupStateTestConstants.TEST_CLEANUP_STATE_FILE_LOCATION);
        CleanupState state = new CleanupState(ImmutableMap.of(CleanupStateTestConstants.KEYSPACE1_TABLE1_KEY, 20L));

        CleanupStateTracker tracker = spy(new CleanupStateTracker(state, persister));
        tracker.createCleanupEntryForTableIfNotExists(CleanupStateTestConstants.KEYSPACE1, CleanupStateTestConstants.TABLE1);
        verify(tracker, times(0)).updateTsForEntry(any(), any());
    }

    @Test
    public void createCleanupEntryForTableSucceedsIfEntryDoesNotExist() throws IOException
    {
        CleanupStatePersister persister =
            new CleanupStatePersister(CleanupStateTestConstants.TEST_CLEANUP_STATE_FILE_LOCATION);
        CleanupState state = new CleanupState(ImmutableMap.of());

        CleanupStateTracker tracker = spy(new CleanupStateTracker(state, persister));
        tracker.createCleanupEntryForTableIfNotExists(CleanupStateTestConstants.KEYSPACE1, CleanupStateTestConstants.TABLE1);
        verify(tracker, times(1))
            .updateTsForEntry(eq(CleanupStateTestConstants.KEYSPACE1_TABLE1_KEY), eq(CleanupStateTracker.MIN_TS.toEpochMilli()));
    }

    @Test
    public void recordSuccessfulCleanupForTableUpdatesEntry() throws IOException
    {
        Long epoch1 = Instant.now().toEpochMilli();
        CleanupStatePersister persister =
            new CleanupStatePersister(CleanupStateTestConstants.TEST_CLEANUP_STATE_FILE_LOCATION);
        CleanupState state = new CleanupState(ImmutableMap.of(CleanupStateTestConstants.KEYSPACE1_TABLE1_KEY, epoch1));

        CleanupStateTracker tracker = spy(new CleanupStateTracker(state, persister));
        assertThat(state.getTableEntries().get(CleanupStateTestConstants.KEYSPACE1_TABLE1_KEY)).isEqualTo(epoch1);
        tracker.recordSuccessfulCleanupForTable(CleanupStateTestConstants.KEYSPACE1, CleanupStateTestConstants.TABLE1);
        assertThat(state.getTableEntries().get(CleanupStateTestConstants.KEYSPACE1_TABLE1_KEY)).isGreaterThan(epoch1);
    }

    @Test
    public void getLastSuccessfulCleanupTsForNodeReturnsMinTsIfNoEntriesExist() throws IOException
    {
        CleanupStatePersister persister =
                new CleanupStatePersister(CleanupStateTestConstants.TEST_CLEANUP_STATE_FILE_LOCATION);
        CleanupState state = new CleanupState(ImmutableMap.of());
        CleanupStateTracker tracker = new CleanupStateTracker(state, persister);

        assertThat(tracker.getLastSuccessfulCleanupTsForNode()).isEqualTo(CleanupStateTracker.MIN_TS);
    }
}
