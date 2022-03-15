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

import org.apache.cassandra.service.StorageServiceMBean;

public class RepairTracker
{
    private final Map<RepairArguments, RepairRunnable> argsToMostRecentRepair;
    private final Map<Integer, RepairRunnable> commandToRepairs;

    public RepairTracker()
    {
        argsToMostRecentRepair = new HashMap<>();
        commandToRepairs = new HashMap<>();
    }

    public synchronized void track(int command, RepairArguments arguments, RepairRunnable task)
    {
        argsToMostRecentRepair.put(arguments, task);
        commandToRepairs.put(command, task);
    }

    public synchronized StorageServiceMBean.ProgressState getRepairState(int command)
    {
        return Optional.ofNullable(commandToRepairs.get(command))
                       .map(RepairRunnable::getCurrentState)
                       .orElse(StorageServiceMBean.ProgressState.UNKNOWN);
    }

    public synchronized Optional<Integer> getInProgressRepair(RepairArguments arguments)
    {
        return Optional.ofNullable(argsToMostRecentRepair.get(arguments))
                       .filter(repair -> StorageServiceMBean.ProgressState.IN_PROGRESS == repair.getCurrentState())
                       .map(RepairRunnable::getCommand);
    }
}
