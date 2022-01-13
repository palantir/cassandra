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

import static org.assertj.core.api.Assertions.assertThat;

import com.codahale.metrics.Timer;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public final class HiccupMeterTest {

    @Test
    public void testHiccupMeter() throws InterruptedException {
        HiccupMeter hiccupMeter = new HiccupMeter();
        Timer timer = JvmMetrics.hiccups;
        hiccupMeter.start();
        TimeUnit.SECONDS.sleep(1);
        assertThat(timer.getCount()).isGreaterThan(90);
        assertThat(timer.getSnapshot().get75thPercentile())
        .isGreaterThan(0)
        .isLessThan(TimeUnit.MILLISECONDS.toNanos(10));
    }
}
