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

package org.apache.cassandra.metrics;

import com.codahale.metrics.Timer;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.time.Duration;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A meter for counting server hiccups; periods where a trivial thread cannot get scheduled (implying that application
 * threads also cannot get scheduled). This is adapted from https://github.com/clojure-goes-fast/jvm-hiccup-meter.
 * <p>
 * Overhead is very low (per second, should represent significantly less CPU time than a single request).
 * <p>
 * It is anticipated that at a high enough resolution, this information can be used both as telemetry, and also to
 * advise QoS systems as to whether the system is under stress.
 */
public final class HiccupMeter {
    private static final Logger log = LoggerFactory.getLogger(HiccupMeter.class);
    private static final ThreadFactory threadFactory = new ThreadFactoryBuilder()
    .setNameFormat("hiccup-meter-%d")
    .setDaemon(true)
    .build();

    private final HiccupMeterTask task;

    public HiccupMeter() {
        this.task = new HiccupMeterTask(JvmMetrics.hiccups);
    }

    public void start() {
        threadFactory.newThread(task).start();
    }

    private static final class HiccupMeterTask implements Runnable {
        private final Timer results;
        private final long resolution;

        private HiccupMeterTask(Timer results) {
            this(results, Duration.ofMillis(10));
        }

        private HiccupMeterTask(Timer results, Duration resolution) {
            this.results = results;
            this.resolution = resolution.toNanos();
        }

        @Override
        public void run() {
            try {
                long shortestObservedDelta = Long.MAX_VALUE;
                long timeBeforeMeasurement = Long.MAX_VALUE;
                while (true) {
                    TimeUnit.NANOSECONDS.sleep(resolution);
                    long timeAfterMeasurement = System.nanoTime();
                    long delta = timeAfterMeasurement - timeBeforeMeasurement;
                    timeBeforeMeasurement = timeAfterMeasurement;

                    boolean notOnFirstIteration = delta > 0;
                    if (notOnFirstIteration) {
                        shortestObservedDelta = Math.min(delta, shortestObservedDelta);
                        long hiccup = delta - shortestObservedDelta;
                        results.update(hiccup, TimeUnit.NANOSECONDS);
                    }
                }
            } catch (InterruptedException e) {
                log.info("Shutting down HiccupMeter thread due to interruption", e);
            }
        }
    }
}