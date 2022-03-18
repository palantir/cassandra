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

import java.util.HashMap;
import java.util.List;
import java.util.Optional;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.service.StorageServiceMBean;

import org.junit.Test;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.utils.progress.ProgressEvent;
import org.apache.cassandra.utils.progress.ProgressEventType;
import org.assertj.core.api.AssertionsForClassTypes;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class RepairTrackerTest
{
    private final RepairOption options = RepairOption.parse(new HashMap<>(), Murmur3Partitioner.instance);
    private final RepairArguments args = new RepairArguments("test", options);

    private static final String TAG1 = "repair:1";
    private static final String TAG2 = "repair:2";
    private static final String TAG3 = "repair:3";

    private static final ProgressEvent PROGRESS = new ProgressEvent(ProgressEventType.PROGRESS, 0, 100, "test");
    private static final ProgressEvent SUCCESS = new ProgressEvent(ProgressEventType.SUCCESS, 0, 100, "test");
    private static final ProgressEvent FAIL = new ProgressEvent(ProgressEventType.ERROR, 0, 100, "test");

    private RepairTracker tracker;

    @Test
    public void progress_noopsWhenNotRepairTag()
    {
        tracker = spy(new RepairTracker());
        tracker.track(1, args);
        reset(tracker);

        tracker.progress("not repair", PROGRESS);
        verify(tracker).progress(any(), any());
        verifyNoMoreInteractions(tracker);
    }

    @Test
    public void progress_cleansCompletedRepairsOnCompletion()
    {
        RepairArguments args1 = new RepairArguments("test", options);
        RepairArguments args2 = new RepairArguments("test2", options);
        RepairArguments args3 = new RepairArguments("test3", options);

        tracker = new RepairTracker();
        tracker.track(1, args1);
        tracker.track(2, args2);
        tracker.track(3, args3);

        tracker.progress(TAG1, PROGRESS);
        tracker.progress(TAG3, PROGRESS);

        assertThat(tracker.getArgsToMostRecentRepair()).isEqualTo(ImmutableMap.of(args1, 1, args2, 2, args3, 3));
        assertThat(tracker.getCommandToProgressState()).isEqualTo(ImmutableMap.of(1,
                                                                                  StorageServiceMBean.ProgressState.IN_PROGRESS, 2,
                                                                                  StorageServiceMBean.ProgressState.UNKNOWN, 3,
                                                                                  StorageServiceMBean.ProgressState.IN_PROGRESS));

        tracker.progress(TAG3, SUCCESS);
        assertThat(tracker.getArgsToMostRecentRepair()).isEqualTo(ImmutableMap.of(args1, 1, args2, 2));
        assertThat(tracker.getCommandToProgressState()).isEqualTo(ImmutableMap.of(1,
                                                                                  StorageServiceMBean.ProgressState.IN_PROGRESS, 2,
                                                                                  StorageServiceMBean.ProgressState.UNKNOWN, 3,
                                                                                  StorageServiceMBean.ProgressState.SUCCEEDED));
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
        tracker.track(1, args);
        tracker.progress(TAG1, FAIL);

        tracker.track(2, args);
        tracker.progress(TAG2, SUCCESS);

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
        tracker = spy(new RepairTracker());

        assertThat(tracker.getInProgressRepair(args)).isEmpty();

        tracker.track(1, args);
        tracker.progress(TAG1, PROGRESS);

        assertThat(tracker.getInProgressRepair(args)).contains(1);

        doReturn(false).when(tracker).isInProgressState(any());
        assertThat(tracker.getInProgressRepair(args)).isEmpty();
    }

    @Test
    public void getInProgressRepair_onlyReturnsMostRecentlyTracked()
    {
        tracker = new RepairTracker();

        tracker.track(1, args);
        tracker.track(2, args);

        tracker.progress(TAG1, PROGRESS);
        tracker.progress(TAG2, PROGRESS);

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

    @Test
    public void maybeUpdateProgressState_switchesToInProgress_onProgress()
    {
        testEventsAndExpectedStates(ImmutableList.of(ProgressEventType.PROGRESS),
                                    ImmutableList.of(StorageServiceMBean.ProgressState.IN_PROGRESS));
    }

    @Test
    public void maybeUpdateProgressState_switchesToInProgress_onStart()
    {
        testEventsAndExpectedStates(ImmutableList.of(ProgressEventType.START),
                                    ImmutableList.of(StorageServiceMBean.ProgressState.IN_PROGRESS));
    }

    @Test
    public void maybeUpdateProgressState_switchesPermanentlyToFailed_onError()
    {
        testEventsAndExpectedStates(ImmutableList.of(ProgressEventType.PROGRESS,
                                                     ProgressEventType.ERROR,
                                                     ProgressEventType.COMPLETE,
                                                     ProgressEventType.PROGRESS,
                                                     ProgressEventType.SUCCESS),
                                    ImmutableList.of(StorageServiceMBean.ProgressState.IN_PROGRESS,
                                                     StorageServiceMBean.ProgressState.FAILED,
                                                     StorageServiceMBean.ProgressState.FAILED,
                                                     StorageServiceMBean.ProgressState.FAILED,
                                                     StorageServiceMBean.ProgressState.FAILED));
    }

    @Test
    public void maybeUpdateProgressState_switchesToFailed_onAbort()
    {
        testEventsAndExpectedStates(ImmutableList.of(ProgressEventType.PROGRESS,
                                                     ProgressEventType.ABORT),
                                    ImmutableList.of(StorageServiceMBean.ProgressState.IN_PROGRESS,
                                                     StorageServiceMBean.ProgressState.FAILED));
    }

    @Test
    public void maybeUpdateProgressState_switchesToFailedFromSucceeded()
    {
        testEventsAndExpectedStates(ImmutableList.of(ProgressEventType.PROGRESS,
                                                     ProgressEventType.SUCCESS,
                                                     ProgressEventType.ERROR),
                                    ImmutableList.of(StorageServiceMBean.ProgressState.IN_PROGRESS,
                                                     StorageServiceMBean.ProgressState.SUCCEEDED,
                                                     StorageServiceMBean.ProgressState.FAILED));
    }

    @Test
    public void maybeUpdateProgressState_switchesToSucceeded()
    {
        testEventsAndExpectedStates(ImmutableList.of(ProgressEventType.PROGRESS,
                                                     ProgressEventType.SUCCESS,
                                                     ProgressEventType.COMPLETE,
                                                     ProgressEventType.PROGRESS),
                                    ImmutableList.of(StorageServiceMBean.ProgressState.IN_PROGRESS,
                                                     StorageServiceMBean.ProgressState.SUCCEEDED,
                                                     StorageServiceMBean.ProgressState.SUCCEEDED,
                                                     StorageServiceMBean.ProgressState.SUCCEEDED));
    }

    @Test
    public void maybeUpdateProgressState_unknownIfCompleteWithoutSuccessOrFailure()
    {
        testEventsAndExpectedStates(ImmutableList.of(ProgressEventType.PROGRESS,
                                                     ProgressEventType.COMPLETE),
                                    ImmutableList.of(StorageServiceMBean.ProgressState.IN_PROGRESS,
                                                     StorageServiceMBean.ProgressState.UNKNOWN));
    }

    @Test
    public void maybeUpdateProgressState_noopsIfCompleteWithSuccessOrFailure()
    {
        testEventsAndExpectedStates(ImmutableList.of(ProgressEventType.PROGRESS,
                                                     ProgressEventType.SUCCESS,
                                                     ProgressEventType.COMPLETE),
                                    ImmutableList.of(StorageServiceMBean.ProgressState.IN_PROGRESS,
                                                     StorageServiceMBean.ProgressState.SUCCEEDED,
                                                     StorageServiceMBean.ProgressState.SUCCEEDED));
        testEventsAndExpectedStates(ImmutableList.of(ProgressEventType.PROGRESS,
                                                     ProgressEventType.ERROR,
                                                     ProgressEventType.COMPLETE),
                                    ImmutableList.of(StorageServiceMBean.ProgressState.IN_PROGRESS,
                                                     StorageServiceMBean.ProgressState.FAILED,
                                                     StorageServiceMBean.ProgressState.FAILED));
    }

    @Test
    public void maybeUpdateProgressState_noops_onNotification()
    {
        testEventsAndExpectedStates(ImmutableList.of(ProgressEventType.PROGRESS,
                                                     ProgressEventType.NOTIFICATION),
                                    ImmutableList.of(StorageServiceMBean.ProgressState.IN_PROGRESS,
                                                     StorageServiceMBean.ProgressState.IN_PROGRESS));
    }

    private void testEventsAndExpectedStates(List<ProgressEventType> progressEvents, List<StorageServiceMBean.ProgressState> expectedStates)
    {
        AssertionsForClassTypes.assertThat(progressEvents.size()).isEqualTo(expectedStates.size());

        tracker = new RepairTracker();
        tracker.track(1, args);

        assertThat(tracker.getRepairState(1)).isEqualTo(StorageServiceMBean.ProgressState.UNKNOWN);

        for (int i = 0; i < progressEvents.size(); i++)
        {
            ProgressEvent event = new ProgressEvent(progressEvents.get(i), 0, 100, "test");
            tracker.progress(TAG1, event);
            assertThat(tracker.getRepairState(1)).isEqualTo(expectedStates.get(i));
        }
    }

    private void assertArgMatch(RepairArguments arg1, RepairArguments arg2, boolean match)
    {
        tracker = new RepairTracker();
        tracker.track(1, arg1);
        tracker.progress(TAG1, PROGRESS);

        Optional<Integer> expected = match ? Optional.of(1) : Optional.empty();
        assertThat(tracker.getInProgressRepair(arg2)).isEqualTo(expected);
    }
}
