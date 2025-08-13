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

package com.palantir.cassandra.logicalts;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class FrozenTimestampTrackerTest
{

    @Test
    public void testLockAndUnlock() throws Exception
    {
        FrozenTimestampTracker tracker = new CollectionBasedFrozenTimestampTracker();

        tracker.advanceFrozenTimestamp(10);

        AutoCloseable closable = tracker.checkAndLockForMutation(15);
        closable.close();
    }

    @Test
    public void testReject() throws Exception
    {
        FrozenTimestampTracker tracker = new CollectionBasedFrozenTimestampTracker();

        tracker.advanceFrozenTimestamp(10);

        assertThatThrownBy(() -> tracker.checkAndLockForMutation(5)).isInstanceOf(IllegalLogicalTimestampException.class);
    }

    @Test
    public void testReportThrowsOnReader() throws Exception
    {
        FrozenTimestampTracker tracker = new CollectionBasedFrozenTimestampTracker();

        tracker.advanceFrozenTimestamp(10);

        AutoCloseable closeable = tracker.checkAndLockForMutation(15);

        assertThatThrownBy(() -> tracker.advanceFrozenTimestamp(20)).isInstanceOf(InProgressMutationConflictException.class);

        closeable.close();

        tracker.advanceFrozenTimestamp(20);
    }

    @Test
    public void testReportThrowsOnMultipleReaders() throws Exception
    {
        FrozenTimestampTracker tracker = new CollectionBasedFrozenTimestampTracker();

        tracker.advanceFrozenTimestamp(10);

        AutoCloseable closeable = tracker.checkAndLockForMutation(15);
        AutoCloseable closeable2 = tracker.checkAndLockForMutation(16);

        assertThatThrownBy(() -> tracker.advanceFrozenTimestamp(20)).isInstanceOf(InProgressMutationConflictException.class);

        closeable.close();

        assertThatThrownBy(() -> tracker.advanceFrozenTimestamp(20)).isInstanceOf(InProgressMutationConflictException.class);

        closeable2.close();

        tracker.advanceFrozenTimestamp(20);
    }
}
