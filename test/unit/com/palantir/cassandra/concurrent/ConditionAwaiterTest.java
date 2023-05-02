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

import org.apache.cassandra.exceptions.ConfigurationException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ConditionAwaiterTest
{
    @Test
    public void testBootstrapManager() throws ConfigurationException, InterruptedException
    {
        final ConditionAwaiter guard = new ConditionAwaiter(false);
        Thread awaitSignalThread = new Thread(guard::await);
        Thread triggerSignalThread = new Thread(guard::proceed);

        awaitSignalThread.start();
        waitUntilNotState(awaitSignalThread, Thread.State.RUNNABLE);
        assertEquals(Thread.State.WAITING, awaitSignalThread.getState());

        triggerSignalThread.start();
        waitUntilNotState(awaitSignalThread, Thread.State.WAITING);
        assertTrue(awaitSignalThread.getState() == Thread.State.RUNNABLE || awaitSignalThread.getState() == Thread.State.TERMINATED);
    }

    private void waitUntilNotState(Thread thread, Thread.State state) throws InterruptedException
    {
        while(thread.getState() == state) {
            Thread.sleep(100);
        }
    }
}
