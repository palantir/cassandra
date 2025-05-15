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

import java.util.concurrent.locks.LockSupport;

/**
 * An abstract signal implementation
 */
public abstract class AbstractSignal implements Signal
{
    public void awaitUninterruptibly()
    {
        boolean interrupted = false;
        while (!isSignalled())
        {
            if (Thread.interrupted())
                interrupted = true;
            LockSupport.park();
        }
        if (interrupted)
            Thread.currentThread().interrupt();
        checkAndClear();
    }

    public void await() throws InterruptedException
    {
        while (!isSignalled())
        {
            checkInterrupted();
            LockSupport.park();
        }
        checkAndClear();
    }

    public boolean awaitUntil(long until) throws InterruptedException
    {
        long now;
        while (until > (now = System.nanoTime()) && !isSignalled())
        {
            checkInterrupted();
            long delta = until - now;
            LockSupport.parkNanos(delta);
        }
        return checkAndClear();
    }

    private void checkInterrupted() throws InterruptedException
    {
        if (Thread.interrupted())
        {
            cancel();
            throw new InterruptedException();
        }
    }
}
