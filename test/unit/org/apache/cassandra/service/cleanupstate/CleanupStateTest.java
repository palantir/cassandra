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

import java.time.Instant;
import java.util.AbstractMap;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class CleanupStateTest
{
    @Test
    public void cleanupStateSuccessfullyUpdates()
    {
        CleanupState state = new CleanupState(ImmutableMap.of());
        assertThat(state.getTableEntries()).isEmpty();
        state.updateTsForEntry(CleanupStateTestConstants.KEYSPACE_TABLE_KEY_1, Instant.ofEpochMilli(10L));
        assertThat(state.getTableEntries()).containsExactly(
            new AbstractMap.SimpleEntry<>(CleanupStateTestConstants.KEYSPACE_TABLE_KEY_1, Instant.ofEpochMilli(10L)));
    }

    @Test
    public void entryExistsReturnsExceptedStatus()
    {
        CleanupState state = new CleanupState(ImmutableMap.of());
        assertThat(state.entryExists(CleanupStateTestConstants.KEYSPACE_TABLE_KEY_1)).isFalse();
        state.updateTsForEntry(CleanupStateTestConstants.KEYSPACE_TABLE_KEY_1, Instant.ofEpochMilli(20L));
        assertThat(state.entryExists(CleanupStateTestConstants.KEYSPACE_TABLE_KEY_1)).isTrue();
    }

    @Test
    public void getMinimumTsReturnsExceptedValues()
    {
        CleanupState state =
            new CleanupState(ImmutableMap.of(CleanupStateTestConstants.KEYSPACE_TABLE_KEY_1, Instant.ofEpochMilli(10L)));
        assertThat(state.getMinimumTsOfAllEntries()).isEqualTo(Instant.ofEpochMilli(10L));
        state.updateTsForEntry(CleanupStateTestConstants.KEYSPACE_TABLE_KEY_2, Instant.ofEpochMilli(20L));
        assertThat(state.getMinimumTsOfAllEntries()).isEqualTo(Instant.ofEpochMilli(10L));
        state.updateTsForEntry(CleanupStateTestConstants.KEYSPACE_TABLE_KEY_1, Instant.ofEpochMilli(20L));
        assertThat(state.getMinimumTsOfAllEntries()).isEqualTo(Instant.ofEpochMilli(20L));
    }

    @Test
    public void throwsWhenTryingToUpdateWithSmallerTs()
    {
        CleanupState state =
            new CleanupState(ImmutableMap.of(CleanupStateTestConstants.KEYSPACE_TABLE_KEY_1, Instant.ofEpochMilli(20L)));
        try
        {
            state.updateTsForEntry(CleanupStateTestConstants.KEYSPACE_TABLE_KEY_1, Instant.ofEpochMilli(10L));
        }
        catch (IllegalArgumentException e) // Expected
        {
            return;
        }
        throw new RuntimeException("Expected test method to throw IllegalArgumentException.");
    }
}
