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

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.Maps;

import com.palantir.cassandra.dht.tokenallocator.GeneticEncoder;
import com.palantir.cassandra.dht.tokenallocator.GeneticOptimizer;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.TokenMetadata;

class GeneticTokenAllocator extends ReplicationAwareTokenAllocator<InetAddress>
{
    GeneticTokenAllocator(NavigableMap<Token, InetAddress> sortedTokens, ReplicationStrategy<InetAddress> strategy, IPartitioner partitioner)
    {
        super(sortedTokens, strategy, partitioner);
    }

    public Collection<Token> addUnit(InetAddress newUnit, int numTokens)
    {
        GeneticOptimizer<List<Token>> optimizer = new GeneticOptimizer<>(
                new TokensGeneticEncoder(partitioner, numTokens),
                tokens -> evaluate(tokens, newUnit));

        optimizer.initialize();
        optimizer.optimize(100);
        return optimizer.getBest();
    }

    // This is super gross in that we have to modify our sortedTokens map to test each candidate, and then revert
    // it afterwards.
    // This is because all the ownership calculation logic uses that member variable rather than being properly
    // abstracted and I don't want to refactor everything right now.
    public double evaluate(Collection<Token> tokens, InetAddress newUnit)
    {
        Map<Object, GroupInfo> groups = Maps.newHashMap();
        Map<InetAddress, UnitInfo<InetAddress>> unitInfos = createUnitInfos(groups);
        unitInfos.put(newUnit, new UnitInfo<>(newUnit, 0, groups, strategy));
        tokens.forEach(token -> sortedTokens.put(token, newUnit));

        // This populates the ownerships in unitInfos.
        // God this code is awful.
        GroupInfo newUnitGroup = groups.get(strategy.getGroup(newUnit));
        createTokenInfos(unitInfos, newUnitGroup);

        // Score is the maximum ownership of any unit in the group.
        double score = unitInfos.values().stream().filter(unit -> unit.group.equals(newUnitGroup)).mapToDouble(unit -> unit.ownership).max().orElse(Double.MAX_VALUE);

        tokens.forEach(sortedTokens::remove);

        return score;
    }

    public int getReplicas()
    {
        return replicas;
    }

    private static class TokensGeneticEncoder implements GeneticEncoder<List<Token>> {
        private static final int tokenSize = 16;
        private final IPartitioner partitioner;
        private final int numTokens;

        TokensGeneticEncoder(IPartitioner partitioner, int numTokens) {
            this.partitioner = partitioner;
            this.numTokens = numTokens;
        }

        public int dnaLength()
        {
            return numTokens * tokenSize;
        }

        public byte[] encode(List<Token> item)
        {
            byte[] dna = new byte[dnaLength()];
            int pos = 0;
            for (Token token : item)
            {
                if (!(token instanceof ByteOrderedPartitioner.BytesToken))
                    throw new IllegalArgumentException("Unsupported token type: " + token.getClass().getName());

                byte[] tokenBytes = (byte[]) token.getTokenValue();
                if (tokenBytes.length != tokenSize)
                    throw new IllegalArgumentException("Token length must be 32 bytes");

                System.arraycopy(tokenBytes, 0, dna, pos, tokenBytes.length);
                pos += tokenSize;
            }
            return dna;
        }

        public List<Token> decode(byte[] dna)
        {
            int numTokens = dna.length / tokenSize;
            List<Token> tokens = new ArrayList<>(numTokens);
            for (int i = 0; i < numTokens; i++)
            {
                byte[] tokenBytes = Arrays.copyOfRange(dna, i * tokenSize, (i + 1) * tokenSize);
                tokens.add(new ByteOrderedPartitioner.BytesToken(tokenBytes));
            }
            return tokens;
        }

        public List<Token> getRandom()
        {
            return IntStream.range(0, numTokens)
                   .mapToObj(_ix -> partitioner.getRandomToken())
                   .collect(Collectors.toList());
        }
    }
}

