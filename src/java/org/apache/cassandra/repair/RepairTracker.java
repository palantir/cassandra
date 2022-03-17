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
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.service.StorageServiceMBean;
import org.apache.cassandra.utils.progress.ProgressEvent;
import org.apache.cassandra.utils.progress.ProgressListener;

public class RepairTracker implements ProgressListener
{
    private static final Logger logger = LoggerFactory.getLogger(RepairTracker.class);
    private final Map<RepairArguments, Integer> argsToMostRecentRepair;
    private final Map<Integer, StorageServiceMBean.ProgressState> commandToProgressState;

    public RepairTracker()
    {
        argsToMostRecentRepair = new HashMap<>();
        commandToProgressState = new HashMap<>();
    }

    public synchronized void track(int command, RepairArguments arguments)
    {
        argsToMostRecentRepair.put(arguments, command);
        commandToProgressState.put(command, StorageServiceMBean.ProgressState.UNKNOWN);
    }

    public synchronized StorageServiceMBean.ProgressState getRepairState(int command)
    {
        return Optional.ofNullable(commandToProgressState.get(command))
                       .orElse(StorageServiceMBean.ProgressState.UNKNOWN);
    }

    public synchronized Optional<Integer> getInProgressRepair(RepairArguments arguments)
    {
        return Optional.ofNullable(argsToMostRecentRepair.get(arguments))
            .filter(command -> isInProgressState(commandToProgressState.get(command)));
    }

    public synchronized void progress(String tag, ProgressEvent event)
    {
        Optional<Integer> repairCommand = RepairRunnable.parseCommandFromTag(tag);
        if (!repairCommand.isPresent())
            return;

        maybeUpdateProgressState(event, repairCommand.get());
    }

    private void updateProgressState(StorageServiceMBean.ProgressState state, int command)
    {
        logger.info("Updating state for repair command {} to {}", command, state);
        commandToProgressState.put(command, state);
        if (isCompleteState(state))
        {
            cleanCompletedRepairs();
        }
    }

    @VisibleForTesting
    void maybeUpdateProgressState(ProgressEvent event, int command)
    {
        StorageServiceMBean.ProgressState currentState = getRepairState(command);
        StorageServiceMBean.ProgressState newState;

        switch (event.getType()) {
            case START:
            case PROGRESS:
                newState = (currentState == StorageServiceMBean.ProgressState.UNKNOWN) ? StorageServiceMBean.ProgressState.IN_PROGRESS : currentState;
                break;
            case ABORT:
            case ERROR:
                newState = StorageServiceMBean.ProgressState.FAILED;
                break;
            case SUCCESS:
                newState = (currentState == StorageServiceMBean.ProgressState.FAILED) ? currentState : StorageServiceMBean.ProgressState.SUCCEEDED;
                break;
            case COMPLETE:
                // Something is wrong if we get a COMPLETE notification when we haven't failed or succeeded
                newState = isCompleteState(currentState) ? currentState : StorageServiceMBean.ProgressState.UNKNOWN;
                break;
            case NOTIFICATION:
                return;
            default:
                logger.error("Unrecognized ProgressEventType. Setting ProgressState to UNKNOWN for repair command {}", command);
                newState = StorageServiceMBean.ProgressState.UNKNOWN;
        }
        if (newState != currentState)
            updateProgressState(newState, command);
    }

    private void cleanCompletedRepairs()
    {
        Set<Integer> completed = commandToProgressState.entrySet().stream()
                                                       .filter(e -> isCompleteState(e.getValue()))
                                                       .map(Map.Entry::getKey)
                                                       .collect(Collectors.toSet());
        removeMatchingCommands(completed, argsToMostRecentRepair);
    }

    private <T> void removeMatchingCommands(Set<Integer> toRemove, Map<T, Integer> map)
    {
        Set<T> keysToRemove = map.entrySet()
                                 .stream()
                                 .filter(e -> toRemove.contains(e.getValue()))
                                 .map(Map.Entry::getKey)
                                 .collect(Collectors.toSet());
        keysToRemove.forEach(map::remove);
    }

    private boolean isCompleteState(StorageServiceMBean.ProgressState state)
    {
        return state == StorageServiceMBean.ProgressState.FAILED || state == StorageServiceMBean.ProgressState.SUCCEEDED;
    }

    @VisibleForTesting
    boolean isInProgressState(StorageServiceMBean.ProgressState state)
    {
        return state == StorageServiceMBean.ProgressState.IN_PROGRESS;
    }

    @VisibleForTesting
    Map<RepairArguments, Integer> getArgsToMostRecentRepair()
    {
        return argsToMostRecentRepair;
    }

    @VisibleForTesting
    Map<Integer, StorageServiceMBean.ProgressState> getCommandToProgressState()
    {
        return commandToProgressState;
    }

}
