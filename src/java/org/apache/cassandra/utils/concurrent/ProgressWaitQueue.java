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

/**
 * Similar in concept to a {@link WaitQueue} but additionally tracks an (assumed) monotonically increasing "progress"
 * quantity.
 * The queue will attempt to only wake threads when the signaled "until" progress quantity reaches or exceeds their
 * requested "waitUntil" value.
 * Note that this is not a strong guarantee, and threads should still wait in a loop to ensure the condition is met.
 */
public interface ProgressWaitQueue
{
    WaitQueue.Signal register(long waitUntil);

    WaitQueue.Signal register(long waitUntil, Timer.Context context);

    void signalUntil(long until);
}
