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
        new File(OpStateTestConstants.TEST_STATE_FILE_LOCATION).delete();
    }

    @Test
    public void keyspaceTableOpStatePersisterSuccessfullyCreateFileWhenDoesNotExist()
    {
        new KeyspaceTableOpStatePersister(OpStateTestConstants.TEST_STATE_FILE_LOCATION);
        assertThat(new File(OpStateTestConstants.TEST_STATE_FILE_LOCATION)).exists();
    }

    @Test
    public void keyspaceTableOpStatePersisterReturnsEmptyMapAtFileCreation()
    {
        KeyspaceTableOpStatePersister persister =
            new KeyspaceTableOpStatePersister(OpStateTestConstants.TEST_STATE_FILE_LOCATION);
        assertThat(persister.readStateFromPersistentLocation()).isEmpty();
    }

    @Test
    public void keyspaceTableOpStatePersisterSuccessfullyUpdatesFile()
    {
        KeyspaceTableOpStatePersister persister =
            new KeyspaceTableOpStatePersister(OpStateTestConstants.TEST_STATE_FILE_LOCATION);
        assertThat(persister.readStateFromPersistentLocation()).isEmpty();

        persister.updateStateInPersistentLocation(
            ImmutableMap.of(OpStateTestConstants.KEYSPACE_TABLE_KEY_1, Instant.ofEpochMilli(10L)));
        assertThat(persister.readStateFromPersistentLocation()).containsExactly(
            new AbstractMap.SimpleEntry<>(OpStateTestConstants.KEYSPACE_TABLE_KEY_1, Instant.ofEpochMilli(10L)));
    }

    @Test
    public void keyspaceTableOpStatePersisterSuccessfullyRetrievesFileWhenExists() throws IOException
    {
        new KeyspaceTableOpStatePersister(OpStateTestConstants.TEST_STATE_FILE_LOCATION);
        File testOpStateFile = new File(OpStateTestConstants.TEST_STATE_FILE_LOCATION);
        assertThat(testOpStateFile).exists();
        OpStateTestConstants.OBJECT_MAPPER.writeValue(
            testOpStateFile, ImmutableMap.of(OpStateTestConstants.KEYSPACE_TABLE_KEY_1.toString(), 10L));

        KeyspaceTableOpStatePersister persister =
            new KeyspaceTableOpStatePersister(OpStateTestConstants.TEST_STATE_FILE_LOCATION);
        assertThat(persister.readStateFromPersistentLocation()).containsExactly(
            new AbstractMap.SimpleEntry<>(OpStateTestConstants.KEYSPACE_TABLE_KEY_1, Instant.ofEpochMilli(10L)));
    }
}
