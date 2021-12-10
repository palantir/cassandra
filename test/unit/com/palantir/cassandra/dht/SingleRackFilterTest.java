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

import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.locator.PropertyFileSnitch;
import org.apache.cassandra.locator.TokenMetadata;

import static org.assertj.core.api.Assertions.assertThat;

public class SingleRackFilterTest
{

    private static final String SOURCE_DC = "dc1";
    private static final String LOCAL_DC = "dc2";

    private AbstractReplicationStrategy replicationStrategy;

    @Before
    public void before() {
        Map<String, String> configOptions = new HashMap<String, String>();
        configOptions.put("dc1", "3");
        configOptions.put("dc2", "3");
        this.replicationStrategy = createNetworkTopologyStrategy(configOptions);
    }

    @Test
    public void create_sortsRacksLexographically()
    {
        assertThat(SingleRackFilter.create(correctTopology(), "dc1", "dc2", "d", replicationStrategy).getMaybeRack()).isNotEmpty().contains("a");
    }

    @Test
    public void create_rackEmptyWhenMismatchedTopologies()
    {
        HashMultimap<String, String> topology = HashMultimap.create();
        topology.put(SOURCE_DC, "c");
        topology.put(SOURCE_DC, "a");
        topology.put(SOURCE_DC, "b");
        topology.put(LOCAL_DC, "f");

        assertThat(SingleRackFilter.create(topology, "dc1", "dc2", "f", replicationStrategy).getMaybeRack()).isEmpty();
    }

    @Test
    public void create_rackEmptyWhenRfMismatches()
    {
        NetworkTopologyStrategy badRf = createNetworkTopologyStrategy(ImmutableMap.of(SOURCE_DC, "3", LOCAL_DC, "2"));
        assertThat(SingleRackFilter.create(correctTopology(), "dc1", "dc2", "f", badRf).getMaybeRack()).isEmpty();
    }

    @Test
    public void create_rackEmptyWhenRfMismatchesTopology()
    {
        NetworkTopologyStrategy badRf = createNetworkTopologyStrategy(ImmutableMap.of(SOURCE_DC, "4", LOCAL_DC, "4"));
        assertThat(SingleRackFilter.create(correctTopology(), "dc1", "dc2", "f", badRf).getMaybeRack()).isEmpty();
    }

    private static HashMultimap<String, String> correctTopology() {
        HashMultimap<String, String> topology = HashMultimap.create();
        topology.put(SOURCE_DC, "c");
        topology.put(SOURCE_DC, "a");
        topology.put(SOURCE_DC, "b");
        topology.put(LOCAL_DC, "f");
        topology.put(LOCAL_DC, "d");
        topology.put(LOCAL_DC, "e");
        return topology;
    }

    private static NetworkTopologyStrategy createNetworkTopologyStrategy(Map<String, String> configOptions) {
        IEndpointSnitch snitch = new PropertyFileSnitch();
        DatabaseDescriptor.setEndpointSnitch(snitch);
        TokenMetadata metadata = new TokenMetadata();
        return new NetworkTopologyStrategy("foo", metadata, snitch, configOptions);
    }
}
