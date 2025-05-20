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

package org.apache.cassandra.concurrent;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.utils.concurrent.ProgressWaitQueue;
import org.apache.cassandra.utils.concurrent.SkipListProgressWaitQueue;
import org.apache.cassandra.utils.concurrent.WaitQueue;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public final class ProgressWaitQueueTest
{
    @Test
    public void testProgressWaitQueue() throws InterruptedException
    {
        ProgressWaitQueue queue = new SkipListProgressWaitQueue();
        AtomicLong progress = new AtomicLong(0);
        List<TestThread> threads = ImmutableList.of(
            new TestThread(queue, progress::get, 0),
            new TestThread(queue, progress::get, 100),
            new TestThread(queue, progress::get, 200),
            new TestThread(queue, progress::get, 300),
            new TestThread(queue, progress::get, 400)
        );

        threads.forEach(Thread::start);
        awaitReady(threads);

        progress.set(100);
        queue.signalUntil(progress.get());

        Util.joinThread(threads.get(0));
        Util.joinThread(threads.get(1));

        assertFalse(threads.get(0).isAlive());
        assertFalse(threads.get(0).isFailed());

        assertFalse(threads.get(1).isAlive());
        assertFalse(threads.get(1).isFailed());

        assertTrue(threads.get(2).isAlive());
        assertTrue(threads.get(3).isAlive());
        assertTrue(threads.get(4).isAlive());

        progress.set(300);
        queue.signalUntil(progress.get());

        Util.joinThread(threads.get(2));
        Util.joinThread(threads.get(3));

        assertFalse(threads.get(2).isAlive());
        assertFalse(threads.get(2).isFailed());

        assertFalse(threads.get(3).isAlive());
        assertFalse(threads.get(3).isFailed());

        assertTrue(threads.get(4).isAlive());

        progress.set(400);
        queue.signalUntil(progress.get());

        Util.joinThread(threads.get(4));
        assertFalse(threads.get(4).isAlive());
        assertFalse(threads.get(4).isFailed());
    }

    private void awaitReady(List<TestThread> threads)
    {
        int iterations = 0;
        while (!threads.stream().allMatch(TestThread::isReady) && iterations < 100)
        {
            try
            {
                Thread.sleep(10);
                iterations += 1;
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    private class TestThread extends Thread
    {
        private final ProgressWaitQueue queue;
        private final LongSupplier progress;
        private final long target;
        private final AtomicBoolean ready = new AtomicBoolean(false);
        private final AtomicBoolean failed = new AtomicBoolean(false);

        private TestThread(ProgressWaitQueue queue, LongSupplier progress, long target)
        {
            this.queue = queue;
            this.progress = progress;
            this.target = target;
        }

        @Override
        public void run()
        {
            WaitQueue.Signal signal = queue.register(target);
            ready.set(true);
            signal.awaitUninterruptibly();

            if (target > progress.getAsLong()) {
                failed.set(true);
            }
        }

        public boolean isReady() { return ready.get(); }
        public boolean isFailed() { return failed.get(); }
    }
}
