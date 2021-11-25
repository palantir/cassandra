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

package com.palantir.cassandra.dht;

import com.google.common.collect.HashMultimap;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SingleRackFilterTest
{

    private static final String SOURCE_DC = "dc1";
    private static final String LOCAL_DC = "dc2";

    @Test
    public void create_sortsRacksLexographically()
    {
        HashMultimap<String, String> topology = HashMultimap.create();
        topology.put(SOURCE_DC, "c");
        topology.put(SOURCE_DC, "a");
        topology.put(SOURCE_DC, "b");
        topology.put(LOCAL_DC, "f");
        topology.put(LOCAL_DC, "d");
        topology.put(LOCAL_DC, "e");

        assertThat(SingleRackFilter.create(topology, "dc1", "dc2", "d").getMaybeRack()).isNotEmpty().contains("a");
    }

    @Test
    public void create_rackEmptyWhenMismatchedTopologies()
    {
        HashMultimap<String, String> topology = HashMultimap.create();
        topology.put(SOURCE_DC, "c");
        topology.put(SOURCE_DC, "a");
        topology.put(SOURCE_DC, "b");
        topology.put(LOCAL_DC, "f");

        assertThat(SingleRackFilter.create(topology, "dc1", "dc2", "f").getMaybeRack()).isEmpty();
    }
}
