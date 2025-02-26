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

package org.apache.cassandra.dht.tokenallocator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.google.common.net.InetAddresses;
import org.junit.Test;

import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.AbstractNetworkTopologySnitch;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.locator.TokenMetadata;

public class PracticalTokenAllocationTest
{
    private static final Pattern PATTERN = Pattern.compile("^\\d{1,3}(?:\\.\\d{1,3}){3}.*");

    @Test
    public void testRing1()
    {
        testTokenRing("example-ring-1.txt");
    }

    @Test
    public void testRing2()
    {
        testTokenRing("example-ring-2.txt");
    }

    @Test
    public void testRing3()
    {
        testTokenRing("example-ring-3.txt");
    }

    private void testTokenRing(String ringFile)
    {
        TestRing testRing = loadNodetoolRing(ringFile);
        AbstractReplicationStrategy rs = testRing.rs;
        TokenMetadata tmd = testRing.tmd;
        TestSnitch snitch = testRing.snitch;
        List<String> uniqueRacks = ImmutableList.copyOf(snitch.racks.values());

        for (int i = 0; i < 100; i++) {
            InetAddress address = InetAddresses.forString(String.format("127.0.0.%d", i));
            Collection<Token> tokens = TokenAllocation.allocateTokens(tmd, rs, new ByteOrderedPartitioner(), address, 32);
            tmd.updateNormalTokens(tokens, address);
            snitch.add(address, uniqueRacks.get(i % uniqueRacks.size()));
        }
    }

    private static TestRing loadNodetoolRing(String resourceName) {
        try
        {
            return parseNodetoolRing(Resources.toString(Resources.getResource("example-rings/" + resourceName), StandardCharsets.UTF_8));
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static TestRing parseNodetoolRing(String output) {

        TokenMetadata tmd = new TokenMetadata();
        Map<InetAddress, String> racks = new HashMap<>();

        String line;
        try (BufferedReader reader = new BufferedReader(new StringReader(output))) {
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                // Skip empty lines and lines that do not look like a node row (expected to start with an IP address)
                if (line.isEmpty() || !PATTERN.matcher(line).matches()) {
                    continue;
                }
                // Split the line into 8 parts. For example:
                // "10.100.241.2    eu-west-2c  Up     Normal  1.16 TB         ?                   003d8e550ef2156238d0bd620d80ab56"
                // will split into:
                // parts[0] = "10.100.241.2"
                // parts[1] = "eu-west-2c"
                // parts[2] = "Up"
                // parts[3] = "Normal"
                // parts[4] = "1.16"
                // parts[5] = "TB"
                // parts[6] = "?"
                // parts[7] = "003d8e550ef2156238d0bd620d80ab56"
                String[] parts = line.split("\\s+", 8);
                if (parts.length < 8) {
                    continue; // or throw an exception if you expect all lines to be valid
                }
                String ipStr = parts[0];
                String rack = parts[1];
                String tokenStr = parts[7];
                InetAddress address = InetAddress.getByName(ipStr);
                // Create a Token instance from the token string.
                Token token = new ByteOrderedPartitioner().getTokenFactory().fromString(tokenStr);

                tmd.updateNormalToken(token, address);
                racks.put(address, rack);
            }
        } catch (Exception e) {
            throw new RuntimeException("Error parsing nodetool ring output", e);
        }

        TestSnitch snitch = new TestSnitch(racks);
        NetworkTopologyStrategy strategy = new NetworkTopologyStrategy("testKeyspace", tmd, snitch, ImmutableMap.of("DC1", "3"));

        return new TestRing(strategy, tmd, snitch);
    }

    private static class TestSnitch extends AbstractNetworkTopologySnitch
    {
        private final Map<InetAddress, String> racks;

        private TestSnitch(Map<InetAddress, String> racks)
        {
            this.racks = racks;
        }

        public String getRack(InetAddress endpoint)
        {
            return racks.get(endpoint);
        }

        public String getDatacenter(InetAddress endpoint)
        {
            return "DC1";
        }

        public void add(InetAddress address, String rack)
        {
            racks.put(address, rack);
        }
    }

    private static class TestRing {
        final AbstractReplicationStrategy rs;
        final TokenMetadata tmd;
        final TestSnitch snitch;

        private TestRing(AbstractReplicationStrategy rs, TokenMetadata tmd, TestSnitch snitch){
            this.rs = rs;
            this.tmd = tmd;
            this.snitch = snitch;
        }
    }
}
