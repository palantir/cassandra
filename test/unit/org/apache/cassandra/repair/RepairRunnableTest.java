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

import java.util.List;

import com.google.common.collect.ImmutableList;
import org.apache.cassandra.service.StorageServiceMBean;
import org.junit.Test;

import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.progress.ProgressEvent;
import org.apache.cassandra.utils.progress.ProgressEventType;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class RepairRunnableTest
{
    @Test
    public void fireProgressEvent_invokesMaybeUpdateProgressState()
    {
        RepairRunnable task = spy(new RepairRunnable(mock(StorageService.class),
                                                     1,
                                                     mock(RepairOption.class),
                                                     "keyspace"));
        ProgressEvent event = new ProgressEvent(ProgressEventType.START, 0, 100, "test");
        task.fireProgressEvent("test", event);
        verify(task).maybeUpdateProgressState(eq(event));
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
        assertThat(progressEvents.size()).isEqualTo(expectedStates.size());
        RepairRunnable task = new RepairRunnable(mock(StorageService.class), 1, mock(RepairOption.class), "keyspace");
        assertThat(task.getCurrentState()).isEqualTo(StorageServiceMBean.ProgressState.UNKNOWN);

        for (int i = 0; i < progressEvents.size(); i++)
        {
            ProgressEvent event = new ProgressEvent(progressEvents.get(i), 0, 100, "test");
            task.maybeUpdateProgressState(event);
            assertThat(task.getCurrentState()).isEqualTo(expectedStates.get(i));
        }
    }
}
