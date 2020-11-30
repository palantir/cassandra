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

package com.palantir.cassandra.concurrent;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;

public class LocalReadRunnableTimeoutWatcher implements Runnable
{
    public static final LocalReadRunnableTimeoutWatcher INSTANCE = new LocalReadRunnableTimeoutWatcher();
    private final ConcurrentHashMap<ReadCommand, Long> readCommandStartTimes = new ConcurrentHashMap<>();
    private static final Logger logger = LoggerFactory.getLogger(LocalReadRunnableTimeoutWatcher.class);

    private LocalReadRunnableTimeoutWatcher() { }

    public void watch(ReadCommand readCommand) {
        logger.trace("Watching read command {} for timeout {}", readCommand, getTimeout());
        readCommandStartTimes.put(readCommand, System.currentTimeMillis());
    }

    private long getTimeout() {
        return DatabaseDescriptor.getReadRpcTimeout();
    }

    public void unwatch(ReadCommand readCommand) {
        Long startTime = readCommandStartTimes.remove(readCommand);
        if (startTime != null) {
            long latency = System.currentTimeMillis() - startTime;
            MessagingService.instance().addLatency(FBUtilities.getBroadcastAddress(), latency);
            logger.warn("Un-watching read command {} for timeout {} ", readCommand, getTimeout());
        }
    }

    public void run()
    {
        logger.trace("Checking read commands {} to see if they've timed out", readCommandStartTimes);
        ArrayList<ReadCommand> timedOutCommands = new ArrayList<>(readCommandStartTimes.size());
        for(Map.Entry<ReadCommand, Long> entry : readCommandStartTimes.entrySet()) {
            if (entry.getValue() + getTimeout() <= System.currentTimeMillis()) {
                timedOutCommands.add(entry.getKey());
            }
        }

        for (ReadCommand command : timedOutCommands) {
            unwatch(command);
        }
    }
}
