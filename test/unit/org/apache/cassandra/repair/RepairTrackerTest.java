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

package org.apache.cassandra.repair;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.KeyCollisionTest;
import org.apache.cassandra.service.StorageServiceMBean;

import org.junit.Test;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.repair.messages.RepairOption;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class RepairTrackerTest
{
    private final RepairOption options = RepairOption.parse(new HashMap<>(), Murmur3Partitioner.instance);
    private final RepairArguments args = new RepairArguments("test", options);
    private RepairTracker tracker;

    @Test
    public void cleanCompletedRepairs_invokedOnTrack()
    {
        tracker = spy(new RepairTracker());
        tracker.track(1, mock(RepairArguments.class), mock(RepairRunnable.class));
        verify(tracker).cleanCompletedRepairs();
    }

    @Test
    public void cleanCompletedRepairs_invokedOnGetRepairState()
    {
        tracker = spy(new RepairTracker());
        tracker.getRepairState(1);
        verify(tracker).cleanCompletedRepairs();
    }

    @Test
    public void cleanCompletedRepairs_invokedOnGetInProgressRepair()
    {
        tracker = spy(new RepairTracker());
        tracker.getInProgressRepair(mock(RepairArguments.class));
        verify(tracker).cleanCompletedRepairs();
    }

    @Test
    public void cleanCompletedRepairs_updatesMapsCorrectly()
    {
        RepairArguments args1 = new RepairArguments("test", options);
        RepairArguments args2 = new RepairArguments("test2", options);
        RepairArguments args3 = new RepairArguments("test3", options);

        RepairRunnable inProgress = mock(RepairRunnable.class);
        doReturn(StorageServiceMBean.ProgressState.IN_PROGRESS).when(inProgress).getCurrentState();

        tracker = new RepairTracker();
        tracker.track(1, args1, inProgress);

        RepairRunnable unknown = mock(RepairRunnable.class);
        doReturn(StorageServiceMBean.ProgressState.UNKNOWN).when(unknown).getCurrentState();
        tracker.track(2, args2, unknown);

        RepairRunnable completed = mock(RepairRunnable.class);
        doReturn(StorageServiceMBean.ProgressState.IN_PROGRESS).when(completed).getCurrentState();
        doReturn(3).when(completed).getCommand();
        tracker.track(3, args3, completed);

        assertThat(tracker.getCommandToCompletedRepairs()).isEmpty();
        assertThat(tracker.getArgsToMostRecentRepair()).isEqualTo(ImmutableMap.of(args1,
                                                                                  inProgress,
                                                                                  args2,
                                                                                  unknown,
                                                                                  args3,
                                                                                  completed));
        assertThat(tracker.getCommandToRepairs()).isEqualTo(ImmutableMap.of(1, inProgress, 2, unknown, 3, completed));

        doReturn(StorageServiceMBean.ProgressState.SUCCEEDED).when(completed).getCurrentState();
        doReturn(true).when(completed).isComplete();
        tracker.cleanCompletedRepairs();

        assertThat(tracker.getCommandToCompletedRepairs()).containsOnlyKeys(3);
        assertThat(tracker.getArgsToMostRecentRepair()).isEqualTo(ImmutableMap.of(args1, inProgress, args2, unknown));
        assertThat(tracker.getCommandToRepairs()).isEqualTo(ImmutableMap.of(1, inProgress, 2, unknown));
    }

    @Test
    public void getRepairState_unknownIfCommandNotTracked()
    {
        tracker = new RepairTracker();
        assertThat(tracker.getRepairState(1)).isEqualTo(StorageServiceMBean.ProgressState.UNKNOWN);
    }

    @Test
    public void getRepairState_returnsExpected()
    {
        tracker = new RepairTracker();
        RepairRunnable task = mock(RepairRunnable.class);
        doReturn(StorageServiceMBean.ProgressState.FAILED).when(task).getCurrentState();
        tracker.track(1, args, task);

        RepairRunnable task2 = mock(RepairRunnable.class);
        doReturn(StorageServiceMBean.ProgressState.SUCCEEDED).when(task2).getCurrentState();
        tracker.track(2, args, task2);

        assertThat(tracker.getRepairState(1)).isEqualTo(StorageServiceMBean.ProgressState.FAILED);
        assertThat(tracker.getRepairState(2)).isEqualTo(StorageServiceMBean.ProgressState.SUCCEEDED);
    }

    @Test
    public void getInProgressRepair_emptyWhenNotTracked()
    {
        tracker = new RepairTracker();
        RepairArguments args = mock(RepairArguments.class);
        assertThat(tracker.getInProgressRepair(args)).isEmpty();
    }

    @Test
    public void getInProgressRepair_onlyReturnsInProgress()
    {
        tracker = new RepairTracker();
        RepairRunnable task = mock(RepairRunnable.class);
        doReturn(1).when(task).getCommand();
        tracker.track(1, args, task);


        Set<StorageServiceMBean.ProgressState> nonInProgress = Arrays.stream(StorageServiceMBean.ProgressState.values())
                                                                     .filter(state -> StorageServiceMBean.ProgressState.IN_PROGRESS != (state))
                                                                     .collect(Collectors.toSet());

        for (StorageServiceMBean.ProgressState state : nonInProgress)
        {
            doReturn(state).when(task).getCurrentState();
            assertThat(tracker.getInProgressRepair(args)).isEmpty();
        }

        doReturn(StorageServiceMBean.ProgressState.IN_PROGRESS).when(task).getCurrentState();
        assertThat(tracker.getInProgressRepair(args)).contains(1);
    }

    @Test
    public void getInProgressRepair_onlyReturnsMostRecentlyTracked()
    {
        tracker = new RepairTracker();
        RepairRunnable task1 = mock(RepairRunnable.class);
        doReturn(1).when(task1).getCommand();
        doReturn(StorageServiceMBean.ProgressState.IN_PROGRESS).when(task1).getCurrentState();
        tracker.track(1, args, task1);

        RepairRunnable task2 = mock(RepairRunnable.class);
        doReturn(StorageServiceMBean.ProgressState.IN_PROGRESS).when(task2).getCurrentState();
        doReturn(2).when(task2).getCommand();
        tracker.track(2, args, task2);

        assertThat(tracker.getInProgressRepair(args)).contains(2);
    }

    @Test
    public void getInProgressRepair_matchesSameArgs()
    {
        RepairArguments newReference = new RepairArguments("test",
                                                           RepairOption.parse(new HashMap<>(),
                                                                              Murmur3Partitioner.instance));
        assertArgMatch(args, newReference, true);
    }

    @Test
    public void getInProgressRepair_doesNotMatchDifferentArgs()
    {

        RepairArguments diffKeyspace = new RepairArguments("diff",
                                                           RepairOption.parse(new HashMap<>(),
                                                                              Murmur3Partitioner.instance));
        assertArgMatch(args, diffKeyspace, false);
        RepairArguments diffOptions = new RepairArguments("test",
                                                          RepairOption.parse(ImmutableMap.of(RepairOption.INCREMENTAL_KEY,
                                                                                             "true",
                                                                                             RepairOption.RANGES_KEY,
                                                                                             "42:42"),
                                                                             Murmur3Partitioner.instance));
        assertArgMatch(args, diffOptions, false);
        RepairArguments diffOptions2 = new RepairArguments("test",
                                                          RepairOption.parse(ImmutableMap.of(RepairOption.INCREMENTAL_KEY,
                                                                                             "true",
                                                                                             RepairOption.RANGES_KEY,
                                                                                             "45:50"),
                                                                             Murmur3Partitioner.instance));
        assertArgMatch(diffOptions2, diffOptions, false);
    }

    private void assertArgMatch(RepairArguments arg1, RepairArguments arg2, boolean match)
    {
        tracker = new RepairTracker();
        RepairRunnable task1 = mock(RepairRunnable.class);
        doReturn(1).when(task1).getCommand();
        doReturn(StorageServiceMBean.ProgressState.IN_PROGRESS).when(task1).getCurrentState();
        tracker.track(1, arg1, task1);

        Optional<Integer> expected = match ? Optional.of(1) : Optional.empty();
        assertThat(tracker.getInProgressRepair(arg2)).isEqualTo(expected);
    }
}
