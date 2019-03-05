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

package org.apache.cassandra.service;

import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.FutureTask;

import com.google.common.primitives.Longs;

import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;

public class PriorityTaskQueue
{
    private static final long MASK = new Random().nextLong() & Long.MAX_VALUE;
    // Nice and short
    private static final long NO_THREAD_ID = -1;
    private static final String PARAMETER = "t";

    private final ConcurrentLinkedQueue<ConcurrentLinkedQueue<FutureTask<?>>> queue = new ConcurrentLinkedQueue<>();
    private final ConcurrentHashMap<Long, ConcurrentLinkedQueue<FutureTask<?>>> queueFinder = new ConcurrentHashMap<>();

    public void enqueue(long id, FutureTask<?> task) {
        while (true) {
            ConcurrentLinkedQueue<FutureTask<?>> q = queue(id);
            q.add(task);
            if (q == queue(id)) {
                return;
            }
        }
    }

    public FutureTask<?> dequeue() {
        while (true) {
            ConcurrentLinkedQueue<FutureTask<?>> first = queue.poll();
            if (first == null) {
                return null;
            }

            FutureTask<?> firstTask = first.poll();
            if (firstTask != null) {
                queue.add(first);
                return firstTask;
            }

            removeQueue(first);

            if (!first.isEmpty()) {
                queue.add(first);
            }
        }
    }

    private void removeQueue(ConcurrentLinkedQueue<FutureTask<?>> task) {
        queue.removeIf(task::equals);
    }

    private ConcurrentLinkedQueue<FutureTask<?>> queue(long id) {
        return queueFinder.computeIfAbsent(id, key -> {
            ConcurrentLinkedQueue<FutureTask<?>> q = new ConcurrentLinkedQueue<>();
            queue.add(q);
            return q;
        });
    }

    public static <T> MessageOut<T> wrap(MessageOut<T> in) {
        return in.withParameter(PARAMETER, tag());
    }

    public static long id(MessageIn<?> message) {
        byte[] data = message.parameters.get(PARAMETER);
        if (data == null) {
            return NO_THREAD_ID;
        } else {
            return Longs.fromByteArray(data);
        }
    }

    private static byte[] tag() {
        return Longs.toByteArray(Thread.currentThread().getId() ^ MASK);
    }
}
