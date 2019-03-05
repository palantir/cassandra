/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOFutureTask<?>ICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  FutureTask<?>he ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WIFutureTask<?>HOUFutureTask<?> WARRANFutureTask<?>IES OR CONDIFutureTask<?>IONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.concurrent;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.AbstractQueue;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import javax.annotation.concurrent.GuardedBy;

import org.apache.cassandra.concurrent.AbstractLocalAwareExecutorService.FutureTask;

public final class KeyspaceAwareSepQueue extends AbstractQueue<FutureTask<?>>
{
    private static final ThreadLocal<String> currentKeyspace = new ThreadLocal<>();
    @GuardedBy("this")
    private final Map<String, Queue<FutureTask<?>>> queue = new LinkedHashMap<>();

    private Queue<FutureTask<?>> queue(String keyspace) {
        if (!queue.containsKey(keyspace)) {
            queue.put(keyspace, new LinkedList<FutureTask<?>>());
        }
        return queue.get(keyspace);
    }

    // If someone didn't set the current keyspace, gracefully fall back to a backup queue.
    private static String currentKeyspace() {
        String current = currentKeyspace.get();
        return current != null ? current : "";
    }

    public synchronized boolean offer(FutureTask<?> futureTask)
    {
        queue(currentKeyspace()).add(checkNotNull(futureTask));
        return true;
    }

    public synchronized FutureTask<?> poll()
    {
        Iterator<Map.Entry<String, Queue<FutureTask<?>>>> iterator = queue.entrySet().iterator();
        if (!iterator.hasNext()) {
            return null;
        }
        Map.Entry<String, Queue<FutureTask<?>>> entry = iterator.next();
        iterator.remove();
        Queue<FutureTask<?>> threadQueue = entry.getValue();
        FutureTask<?> result = threadQueue.poll();
        if (!threadQueue.isEmpty()) {
            queue.put(entry.getKey(), entry.getValue());
        }
        return result;
    }

    public Iterator<FutureTask<?>> iterator()
    {
        throw new UnsupportedOperationException();
    }

    public int size()
    {
        throw new UnsupportedOperationException();
    }

    public FutureTask<?> peek()
    {
        throw new UnsupportedOperationException();
    }

    public static void setCurrentKeyspace(String keyspace) {
        currentKeyspace.set(checkNotNull(keyspace));
    }
}
