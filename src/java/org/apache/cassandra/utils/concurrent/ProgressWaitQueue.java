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

import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.locks.LockSupport;

import com.google.common.collect.ImmutableMap;

import com.codahale.metrics.Timer;
import com.palantir.tracing.CloseableTracer;
import com.palantir.tracing.DetachedSpan;
import com.palantir.tracing.Tracer;

public final class ProgressWaitQueue
{
    private static final int CANCELLED = -1;
    private static final int SIGNALLED = 1;
    private static final int NOT_SET = 0;

    private static final AtomicIntegerFieldUpdater signalledUpdater = AtomicIntegerFieldUpdater.newUpdater(ProgressWaitQueue.RegisteredSignal.class, "state");

    private final ConcurrentNavigableMap<Long, RegisteredSignal> waiters = new ConcurrentSkipListMap<>();

    public WaitQueue.Signal register(long waitUntil)
    {
        RegisteredSignal signal = new RegisteredSignal();
        waiters.put(waitUntil, signal);
        return signal;
    }

    public WaitQueue.Signal register(long waitUntil, Timer.Context context)
    {
        assert context != null;
        RegisteredSignal signal = new TimedSignal(context);
        waiters.put(waitUntil, signal);
        return signal;
    }

    public void signalUntil(long until) {
        ConcurrentNavigableMap<Long, RegisteredSignal> dueWaiters = waiters.headMap(until, true);
        try (CloseableTracer ignored = CloseableTracer.startSpan("ProgressWaitQueue#signalAll", ImmutableMap.of("numWaiters", Integer.toString(waiters.size()), "numDueWaiters", Integer.toString(dueWaiters.size()))))
        {
            Iterator<RegisteredSignal> iter = dueWaiters.values().iterator();
            while (iter.hasNext())
            {
                RegisteredSignal signal = iter.next();
                signal.signal();
                iter.remove();
            }
        }
    }

    private void cleanUpCancelled()
    {
        waiters.values().removeIf(RegisteredSignal::isCancelled);
    }

    /**
     * A signal registered with this WaitQueue
     */
    private class RegisteredSignal extends WaitQueue.AbstractSignal
    {
        private final DetachedSpan span = DetachedSpan.start("ProgressWaitQueue#parked");
        private volatile Thread thread = Thread.currentThread();
        volatile int state;

        public boolean isSignalled()
        {
            return state == SIGNALLED;
        }

        public boolean isCancelled()
        {
            return state == CANCELLED;
        }

        public boolean isSet()
        {
            return state != NOT_SET;
        }

        private Thread signal()
        {
            if (!isSet() && signalledUpdater.compareAndSet(this, NOT_SET, SIGNALLED))
            {
                Thread thread = this.thread;
                LockSupport.unpark(thread);
                if (Tracer.hasTraceId()) {
                    this.span.complete(ImmutableMap.of("childTraceIds", Tracer.getTraceId()));
                } else {
                    this.span.complete();
                }
                this.thread = null;
                return thread;
            }
            return null;
        }

        public boolean checkAndClear()
        {
            if (!isSet() && signalledUpdater.compareAndSet(this, NOT_SET, CANCELLED))
            {
                thread = null;
                cleanUpCancelled();
                return false;
            }
            // must now be signalled assuming correct API usage
            return true;
        }

        /**
         * Should only be called by the registered thread. Indicates the signal can be retired,
         */
        public void cancel()
        {
            if (isCancelled())
                return;
            if (!signalledUpdater.compareAndSet(this, NOT_SET, CANCELLED))
            {
                // must already be signalled - switch to cancelled
                state = CANCELLED;
            }
            thread = null;
            cleanUpCancelled();
        }
    }

    /**
     * A RegisteredSignal that stores a TimerContext, and stops the timer when either cancelled or
     * finished waiting. i.e. if the timer is started when the signal is registered it tracks the
     * time in between registering and invalidating the signal.
     */
    private final class TimedSignal extends ProgressWaitQueue.RegisteredSignal
    {
        private final Timer.Context context;

        private TimedSignal(Timer.Context context)
        {
            this.context = context;
        }

        @Override
        public boolean checkAndClear()
        {
            context.stop();
            return super.checkAndClear();
        }

        @Override
        public void cancel()
        {
            if (!isCancelled())
            {
                context.stop();
                super.cancel();
            }
        }
    }
}
