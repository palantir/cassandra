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

import java.time.Instant;
import java.util.AbstractMap;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;

public class KeyspaceTableOpStateCacheTest
{
    @Test
    public void stateSuccessfullyUpdates()
    {
        KeyspaceTableOpStateCache state = new KeyspaceTableOpStateCache(ImmutableMap.of());
        assertThat(state.getTableEntries()).isEmpty();
        state.updateTsForEntry(OpStateTestConstants.KEYSPACE_TABLE_KEY_1, Instant.ofEpochMilli(10L));
        assertThat(state.getTableEntries()).containsExactly(
            new AbstractMap.SimpleEntry<>(OpStateTestConstants.KEYSPACE_TABLE_KEY_1, Instant.ofEpochMilli(10L)));
    }

    @Test
    public void entryExistsReturnsExceptedStatus()
    {
        KeyspaceTableOpStateCache state = new KeyspaceTableOpStateCache(ImmutableMap.of());
        assertThat(state.entryExists(OpStateTestConstants.KEYSPACE_TABLE_KEY_1)).isFalse();
        state.updateTsForEntry(OpStateTestConstants.KEYSPACE_TABLE_KEY_1, Instant.ofEpochMilli(20L));
        assertThat(state.entryExists(OpStateTestConstants.KEYSPACE_TABLE_KEY_1)).isTrue();
    }

    @Test
    public void getMinimumTsReturnsExceptedValues()
    {
        KeyspaceTableOpStateCache state =
        Mockito.spy(new KeyspaceTableOpStateCache(ImmutableMap.of(OpStateTestConstants.KEYSPACE_TABLE_KEY_1, Instant.ofEpochMilli(10L))));
        doReturn(OpStateTestConstants.KEYSPACE_TABLE_VALID_ENTRIES).when(state).getValidKeyspaceTableEntries();
        // Missing entry for KEYSPACE_TABLE_KEY_2, getMinimumTsOfAllEntries should return an empty value
        assertThat(state.getMinimumTsOfAllEntries().orElse(null)).isNull();
        state.updateTsForEntry(OpStateTestConstants.KEYSPACE_TABLE_KEY_2, Instant.ofEpochMilli(20L));
        assertThat(state.getMinimumTsOfAllEntries().orElse(null)).isEqualTo(Instant.ofEpochMilli(10L));
        state.updateTsForEntry(OpStateTestConstants.KEYSPACE_TABLE_KEY_1, Instant.ofEpochMilli(20L));
        assertThat(state.getMinimumTsOfAllEntries().orElse(null)).isEqualTo(Instant.ofEpochMilli(20L));
    }

    @Test
    public void getMinimumTsIgnoresInvalidEntries()
    {
        KeyspaceTableOpStateCache state =
        Mockito.spy(new KeyspaceTableOpStateCache(ImmutableMap.of(OpStateTestConstants.KEYSPACE_TABLE_KEY_1, Instant.ofEpochMilli(10L))));
        state.updateTsForEntry(OpStateTestConstants.KEYSPACE_TABLE_KEY_2, Instant.ofEpochMilli(20L));

        doReturn(ImmutableSet.of(OpStateTestConstants.KEYSPACE_TABLE_KEY_2)).when(state).getValidKeyspaceTableEntries();
        assertThat(state.getMinimumTsOfAllEntries().orElse(null)).isEqualTo(Instant.ofEpochMilli(20L));
    }

    @Test
    public void throwsWhenTryingToUpdateWithSmallerTs()
    {
        KeyspaceTableOpStateCache state =
        new KeyspaceTableOpStateCache(ImmutableMap.of(OpStateTestConstants.KEYSPACE_TABLE_KEY_1, Instant.ofEpochMilli(20L)));
        try
        {
            state.updateTsForEntry(OpStateTestConstants.KEYSPACE_TABLE_KEY_1, Instant.ofEpochMilli(10L));
        }
        catch (IllegalArgumentException e) // Expected
        {
            return;
        }
        throw new RuntimeException("Expected test method to throw IllegalArgumentException.");
    }
}