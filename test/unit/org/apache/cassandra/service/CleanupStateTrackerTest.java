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

package org.apache.cassandra.service;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.AbstractMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Test;

import org.codehaus.jackson.map.ObjectMapper;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;

public class CleanupStateTrackerTest
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final String KEYSPACE1 = "keyspace1";
    private static final String TABLE1 = "table1";
    private static final String KEYSPACE1_TABLE1_KEY = KEYSPACE1 + ":" +TABLE1;

    private static final String KEYSPACE2 = "keyspace2";
    private static final String TABLE2 = "table2";
    private static final String KEYSPACE2_TABLE2_KEY = KEYSPACE2 + ":" +TABLE2;

    private static final String TEST_CLEANUP_STATE_FILE_LOCATION =
    System.getProperty("user.dir") + "/test/resources/" + CleanupStateTracker.CLEANUP_STATE_FILE_NAME;


    CleanupStateTracker tracker = Mockito.mock(CleanupStateTracker.class);

    @After
    public void afterEach(){
        new File(TEST_CLEANUP_STATE_FILE_LOCATION).delete();
    }

    @Test
    public void cleanupStateFileCreationSuccessfulWhenCorrectPathProvided() throws IOException
    {
        Mockito.doReturn(TEST_CLEANUP_STATE_FILE_LOCATION).when(tracker).cleanupStateFileLocation();
        Mockito.doCallRealMethod().when(tracker).createOrRetrieveCleanupStateFile();

        tracker.createOrRetrieveCleanupStateFile();
        assertThat(new File(TEST_CLEANUP_STATE_FILE_LOCATION)).exists();
    }

    @Test
    public void cleanupStateIsBlankSlateWhenNewFileCreated() throws IOException
    {
        Mockito.doReturn(TEST_CLEANUP_STATE_FILE_LOCATION).when(tracker).cleanupStateFileLocation();
        Mockito.doCallRealMethod().when(tracker).createOrRetrieveCleanupStateFile();

        File cleanupStateFile = tracker.createOrRetrieveCleanupStateFile();
        CleanupStateTracker.CleanupState cleanupState = new CleanupStateTracker.CleanupState(cleanupStateFile);

        assertThat(cleanupState.getTableEntries()).isEmpty();
    }

    @Test
    public void cleanupStateSuccessfullyRetrievedWhenFileExists() throws IOException
    {
        Mockito.doReturn(TEST_CLEANUP_STATE_FILE_LOCATION).when(tracker).cleanupStateFileLocation();
        Mockito.doCallRealMethod().when(tracker).createOrRetrieveCleanupStateFile();

        File cleanupStateFile = tracker.createOrRetrieveCleanupStateFile();
        Map<String, Long> testEntries = ImmutableMap.of(KEYSPACE1_TABLE1_KEY, 10L, KEYSPACE2_TABLE2_KEY, 20L);
        OBJECT_MAPPER.writeValue(cleanupStateFile, testEntries);

        CleanupStateTracker.CleanupState cleanupState = new CleanupStateTracker.CleanupState(cleanupStateFile);
        assertThat(cleanupState.getTableEntries()).isEqualTo(testEntries);
    }

    @Test
    public void cleanupStateEntriesSuccessfullyUpdated() throws IOException
    {
        Mockito.doReturn(TEST_CLEANUP_STATE_FILE_LOCATION).when(tracker).cleanupStateFileLocation();
        Mockito.doCallRealMethod().when(tracker).createOrRetrieveCleanupStateFile();

        File cleanupStateFile = tracker.createOrRetrieveCleanupStateFile();
        CleanupStateTracker.CleanupState cleanupState = new CleanupStateTracker.CleanupState(cleanupStateFile);

        assertThat(cleanupState.entryExists(KEYSPACE1, TABLE1)).isFalse();
        assertThat(cleanupState.getTableEntries()).isEmpty();

        cleanupState.updateTsForEntry(KEYSPACE1, TABLE1, Instant.ofEpochMilli(10L));
        assertThat(cleanupState.entryExists(KEYSPACE1, TABLE1)).isTrue();
        assertThat(cleanupState.getTableEntries())
            .containsExactly(new AbstractMap.SimpleEntry<>(KEYSPACE1_TABLE1_KEY, 10L));
    }

    @Test
    public void cleanupStateFileIsUpdatedAtEachWrite() throws IOException
    {
        Mockito.doReturn(TEST_CLEANUP_STATE_FILE_LOCATION).when(tracker).cleanupStateFileLocation();
        Mockito.doCallRealMethod().when(tracker).createOrRetrieveCleanupStateFile();

        File cleanupStateFile = tracker.createOrRetrieveCleanupStateFile();
        CleanupStateTracker.CleanupState cleanupState = new CleanupStateTracker.CleanupState(cleanupStateFile);

        assertThat(cleanupState.getTableEntries()).isEmpty();
        cleanupState.updateTsForEntry(KEYSPACE1, TABLE1, Instant.ofEpochMilli(10L));
        assertThat(cleanupState.entryExists(KEYSPACE1, TABLE1)).isTrue();
        assertThat(cleanupState.convertObjectMapTypetoStringLong(OBJECT_MAPPER.readValue(cleanupStateFile, Map.class)))
            .isEqualTo(cleanupState.getTableEntries());
    }

    @Test
    public void minFunctionReturnsExpectedValues() throws IOException
    {
        Mockito.doReturn(TEST_CLEANUP_STATE_FILE_LOCATION).when(tracker).cleanupStateFileLocation();
        Mockito.doCallRealMethod().when(tracker).createOrRetrieveCleanupStateFile();

        File cleanupStateFile = tracker.createOrRetrieveCleanupStateFile();
        CleanupStateTracker.CleanupState cleanupState = new CleanupStateTracker.CleanupState(cleanupStateFile);

        assertThat(cleanupState.getMinimumTsOfAllEntries()).isNull();

        Instant instantAtTen = Instant.ofEpochMilli(10L);
        cleanupState.updateTsForEntry(KEYSPACE1, TABLE1, instantAtTen);
        assertThat(cleanupState.getMinimumTsOfAllEntries()).isEqualTo(instantAtTen);

        Instant instantAtTwenty = Instant.ofEpochMilli(20L);
        cleanupState.updateTsForEntry(KEYSPACE2, TABLE2, instantAtTwenty);
        assertThat(cleanupState.getMinimumTsOfAllEntries()).isEqualTo(instantAtTen);

        cleanupState.updateTsForEntry(KEYSPACE1, TABLE1, instantAtTwenty);
        assertThat(cleanupState.getMinimumTsOfAllEntries()).isEqualTo(instantAtTwenty);
    }

    @Test
    public void throwsErrorWhenTryingToUpdateEntryWithSmallerTs() throws IOException
    {
        Mockito.doReturn(TEST_CLEANUP_STATE_FILE_LOCATION).when(tracker).cleanupStateFileLocation();
        Mockito.doCallRealMethod().when(tracker).createOrRetrieveCleanupStateFile();

        File cleanupStateFile = tracker.createOrRetrieveCleanupStateFile();
        CleanupStateTracker.CleanupState cleanupState = new CleanupStateTracker.CleanupState(cleanupStateFile);

        assertThat(cleanupState.getMinimumTsOfAllEntries()).isNull();

        Instant instantAtTwenty = Instant.ofEpochMilli(20L);
        cleanupState.updateTsForEntry(KEYSPACE1, TABLE1, instantAtTwenty);
        assertThat(cleanupState.getTableEntries())
            .containsExactly(new AbstractMap.SimpleEntry<>(KEYSPACE1_TABLE1_KEY, 20L));

        Instant instantAtTen = Instant.ofEpochMilli(10L);
        try
        {
            cleanupState.updateTsForEntry(KEYSPACE1, TABLE1, instantAtTen);
        }
        catch (IllegalArgumentException e) // Expected
        {
            return;
        }
        throw new RuntimeException("Expected method to throw IllegalArgumentException.");
    }
}
