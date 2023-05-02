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

import com.google.common.util.concurrent.Monitor;

public class ConditionAwaiter
{
    private volatile boolean allowedToProceed = false;
    private final Monitor monitor = new Monitor();
    private final Monitor.Guard isAllowedToProceed = getNewGuard(monitor);
    private final boolean disabled;

    public ConditionAwaiter(boolean disabled)
    {
        this.disabled = disabled;
    }

    public void proceed() {
        monitor.enter();
        try {
            allowedToProceed = true;
        } finally {
            monitor.leave();
        }
    }

    public void await() {
        try {
            if (disabled)
                return;
            monitor.enterWhen(isAllowedToProceed);
        } catch (InterruptedException | IllegalStateException e) {
            throw new RuntimeException("Failed while waiting for an operator command", e);
        }
    }

    private Monitor.Guard getNewGuard(Monitor monitor) {
        return new Monitor.Guard(monitor) {
            @Override
            public boolean isSatisfied() {
                return allowedToProceed;
            }
        };
    }
}
