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

package org.apache.cassandra.utils.concurrent;

import com.codahale.metrics.Timer;

public final class ProgressWaitQueueAdapter implements ProgressWaitQueue
{
    private final WaitQueue waitQueue;

    public ProgressWaitQueueAdapter(WaitQueue waitQueue)
    {
        this.waitQueue = waitQueue;
    }

    public WaitQueue.Signal register(long _waitUntil)
    {
        return waitQueue.register();
    }

    public WaitQueue.Signal register(long _waitUntil, Timer.Context context)
    {
        return waitQueue.register(context);
    }

    public void signalUntil(long _until)
    {
        waitQueue.signalAll();
    }
}
