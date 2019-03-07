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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.AbstractLocalAwareExecutorService.FutureTask;
import org.apache.cassandra.db.AbstractRangeCommand;
import org.apache.cassandra.service.IReadCommand;

public final class ParameterizedSepQueue extends AbstractQueue<FutureTask<?>>
{
    private static final Logger log = LoggerFactory.getLogger(ParameterizedSepQueue.class);
    private static final ThreadLocal<String> queueingParameter = new ThreadLocal<>();
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
        String current = queueingParameter.get();
        queueingParameter.remove();
        if (current == null) {
            log.info("Saw a command where the current keyspace was not set, which indicates a bug");
            return "";
        }
        return current;
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

    /**
     * Hypothesis: Gets happen on all tables, so you want to prioritize per keyspace. Scans happen on individual
     * tables, and we'd rather indicate that gets to a specific table are expensive rather than all tables; better
     * to take out reads to a single table than all.
     */
    public static void setQueueingParameter(IReadCommand read) {
        if (read instanceof AbstractRangeCommand) {
            queueingParameter.set(((AbstractRangeCommand) read).columnFamily);
        } else {
            queueingParameter.set(read.getKeyspace());
        }
    }
}
