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
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;

import org.apache.commons.collections.map.UnmodifiableMap;

import org.apache.cassandra.service.StorageServiceMBean;

public class RepairTracker
{
    private final Map<RepairArguments, RepairRunnable> argsToMostRecentRepair;
    private final Map<Integer, RepairRunnable> commandToRepairs;
    private final Map<Integer, StorageServiceMBean.ProgressState> commandToCompletedRepairs;

    public RepairTracker()
    {
        argsToMostRecentRepair = new HashMap<>();
        commandToRepairs = new HashMap<>();
        commandToCompletedRepairs = new HashMap<>();
    }

    public synchronized void track(int command, RepairArguments arguments, RepairRunnable task)
    {
        cleanCompletedRepairs();
        argsToMostRecentRepair.put(arguments, task);
        commandToRepairs.put(command, task);
    }

    public synchronized StorageServiceMBean.ProgressState getRepairState(int command)
    {
        cleanCompletedRepairs();
        StorageServiceMBean.ProgressState completedState = Optional.ofNullable(commandToCompletedRepairs.get(command))
                                                                   .orElse(StorageServiceMBean.ProgressState.UNKNOWN);
        return Optional.ofNullable(commandToRepairs.get(command))
                       .map(RepairRunnable::getCurrentState)
                       .orElse(completedState);
    }

    public synchronized Optional<Integer> getInProgressRepair(RepairArguments arguments)
    {
        cleanCompletedRepairs();
        return Optional.ofNullable(argsToMostRecentRepair.get(arguments))
                       .filter(repair -> StorageServiceMBean.ProgressState.IN_PROGRESS == repair.getCurrentState())
                       .map(RepairRunnable::getCommand);
    }

    @VisibleForTesting
    Map<RepairArguments, RepairRunnable> getArgsToMostRecentRepair()
    {
        return argsToMostRecentRepair;
    }

    @VisibleForTesting
    Map<Integer, RepairRunnable> getCommandToRepairs()
    {
        return commandToRepairs;
    }

    @VisibleForTesting
    Map<Integer, StorageServiceMBean.ProgressState> getCommandToCompletedRepairs()
    {
        return commandToCompletedRepairs;
    }

    @VisibleForTesting
    void cleanCompletedRepairs()
    {
        Map<Integer, StorageServiceMBean.ProgressState> newlyCompleted =
                        Stream.concat(argsToMostRecentRepair.values().stream(), commandToRepairs.values().stream())
                              .filter(RepairRunnable::isComplete)
                              .distinct()
                              .collect(Collectors.toMap(RepairRunnable::getCommand,
                                                        RepairRunnable::getCurrentState));

        commandToCompletedRepairs.putAll(newlyCompleted);
        removeMatchingCommands(newlyCompleted.keySet(), argsToMostRecentRepair);
        removeMatchingCommands(newlyCompleted.keySet(), commandToRepairs);
    }

    private <T> void removeMatchingCommands(Set<Integer> toRemove, Map<T, RepairRunnable> map)
    {
        Set<T> keysToRemove = map.entrySet()
                                 .stream()
                                 .filter(e -> toRemove.contains(e.getValue().getCommand()))
                                 .map(Map.Entry::getKey)
                                 .collect(Collectors.toSet());
        keysToRemove.forEach(map::remove);
    }
}
