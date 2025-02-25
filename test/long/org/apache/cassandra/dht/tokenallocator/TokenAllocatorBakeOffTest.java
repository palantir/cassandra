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
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import com.google.common.io.Resources;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.Token;

public class TokenAllocatorBakeOffTest
{
    @Test
    public void testGalleonAlta()
    {
        testTokenRing("galleon-alta.txt");
    }

    @Test
    public void testRosewoodAlta()
    {
        testTokenRing("rosewood-alta.txt");
    }

    @Test
    public void testTaffeta()
    {
        testTokenRing("taffeta.txt");
    }

    private void testTokenRing(String ringFile)
    {
        double cass3Score = evaluateAllocator(ringFile, TokenAllocatorBakeOffTest::cass3Allocator);
        System.out.println("Cassandra 3 score: " + cass3Score);

        double geneticScore = evaluateAllocator(ringFile, TokenAllocatorBakeOffTest::geneticAllocator);
        System.out.println("Genetic score: " + geneticScore);

        if (cass3Score < geneticScore) {
            Assert.fail("Cassandra 3 allocator is better than genetic allocator");
        }
    }

    private double evaluateAllocator(String nodetoolRingFile, Function<TestRing, TokenAllocator<InetAddress>> allocatorFactory) {
        TestRing ring = loadNodetoolRing(nodetoolRingFile);

        Set<String> groups = new HashSet<>(ring.groups.values());

        AtomicInteger ix = new AtomicInteger(1);
        return groups.stream()
            .sorted()
        .mapToDouble(group -> {
            InetAddress newUnit = address(String.format("127.0.0.%d", ix.getAndIncrement()));
            ring.groups.put(newUnit, group);
            Collection<Token> tokens = allocatorFactory.apply(ring).addUnit(newUnit, 32);
            double score = geneticAllocator(ring).evaluate(tokens, newUnit);
            tokens.forEach(token -> ring.tokens.put(token, newUnit));
            return score;
        }).max().orElse(Double.MAX_VALUE);
    }

    private static ReplicationAwareTokenAllocator<InetAddress> cass3Allocator(TestRing ring) {
        return new ReplicationAwareTokenAllocator<>(ring.tokens, ring, new ByteOrderedPartitioner());
    }

    private static GeneticTokenAllocator geneticAllocator(TestRing ring) {
        return new GeneticTokenAllocator(ring.tokens, ring, new ByteOrderedPartitioner());
    }

    private static InetAddress address(String ip) {
        try
        {
            return InetAddress.getByName(ip);
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static TestRing loadNodetoolRing(String resourceName) {
        try
        {
            return parseNodetoolRing(Resources.toString(Resources.getResource(resourceName), StandardCharsets.UTF_8));
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static TestRing parseNodetoolRing(String output) {
        NavigableMap<Token, InetAddress> tokenMap = new TreeMap<>();
        Map<InetAddress, String> groups = new HashMap<>();
        // Set a default replica count (change as needed)
        int replicas = 3;

        BufferedReader reader = new BufferedReader(new StringReader(output));
        String line;
        try {
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                // Skip empty lines and lines that do not look like a node row (expected to start with an IP address)
                if (line.isEmpty() || !line.matches("^\\d{1,3}(?:\\.\\d{1,3}){3}.*")) {
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

                tokenMap.put(token, address);
                groups.put(address, rack);
            }
        } catch (Exception e) {
            throw new RuntimeException("Error parsing nodetool ring output", e);
        }
        return new TestRing(tokenMap, groups, replicas);
    }

    private static class TestRing implements ReplicationStrategy<InetAddress> {
        private final NavigableMap<Token, InetAddress> tokens;
        private final Map<InetAddress, String> groups;
        private final int replicas;

        private TestRing(NavigableMap<Token, InetAddress> tokens, Map<InetAddress, String> groups, int replicas)
        {
            this.tokens = tokens;
            this.groups = groups;
            this.replicas = replicas;
        }

        public int replicas()
        {
            return replicas;
        }

        public Object getGroup(InetAddress inetAddress)
        {
            return groups.get(inetAddress);
        }
    }
}
