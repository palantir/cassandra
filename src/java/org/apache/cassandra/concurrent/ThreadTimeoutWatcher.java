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

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThreadTimeoutWatcher implements Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(ThreadTimeoutWatcher.class);
    public static final ThreadTimeoutWatcher INSTANCE = new ThreadTimeoutWatcher();
    private final ConcurrentHashMap<Thread, Long> threadsToWatch = new ConcurrentHashMap<>();

    private ThreadTimeoutWatcher() {
        logger.info("STARTING THREAD WATCHER");
    }

    public void watchThread(long timeout) {
        watchThread(Thread.currentThread(), System.currentTimeMillis() + timeout);
    }

    public void watchThread(Thread thread, long timeout) {
        logger.info("ADDED THREAD TO WATCH", thread.getId(), timeout);
        threadsToWatch.put(thread, System.currentTimeMillis() + timeout);
    }

    public void unwatchThread(Thread thread) {
        logger.info("REMOVED THREAD TO WATCH", thread.getId());
        threadsToWatch.remove(thread);
    }

    public void unwatchThread() {
        unwatchThread(Thread.currentThread());
    }

    public void run()
    {
        while(true) {
            logger.info("Checking to see if threads need to be interrupted.", threadsToWatch);
            ArrayList<Thread> threadsToWake = new ArrayList(threadsToWatch.size());
            for (Map.Entry<Thread, Long> entry : threadsToWatch.entrySet()) {
                if (System.currentTimeMillis() >= entry.getValue()) {
                    threadsToWake.add(entry.getKey());
                }
            }
            for(Thread thread : threadsToWake) {
                if (threadsToWatch.remove(thread) != null) {
                    thread.interrupt();
                    logger.warn("Evicted thread {} due to hitting timeout", thread.getId());
                }
            }

            try
            {
                Thread.sleep(1000L);
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
            }
        }
    }
}
