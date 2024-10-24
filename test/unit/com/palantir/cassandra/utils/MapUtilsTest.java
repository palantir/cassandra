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

package com.palantir.cassandra.utils;

import java.net.InetAddress;
import java.util.*;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.locator.PendingRangeMaps;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.Util.token;
import static org.assertj.core.api.Assertions.assertThat;

public final class MapUtilsTest
{
    private static final String KEY_1 = "key1";

    private static final Token TOKEN_1 = token("token1");

    private static final Token TOKEN_2 = token("token2");

    private InetAddress ep1;

    private InetAddress ep2;

    private Multimap<String, Token> v1;

    private Multimap<String, Token> v2;

    private Multimap<Range<Token>, InetAddress> addressRange;

    @Before
    public void before() throws Exception
    {
        v1 = HashMultimap.<String, Token>create();
        v2 = HashMultimap.<String, Token>create();
        addressRange = HashMultimap.create();

        ep1 = InetAddress.getByName("127.0.0.1");
        ep2 = InetAddress.getByName("127.0.0.2");
    }

    @Test
    public void symmetricDifference_sameMapContentHasEmptySet()
    {
        putAll(v1, KEY_1, TOKEN_1);
        putAll(v2, KEY_1, TOKEN_1);
        Map<String, Pair<Set<Token>, Set<Token>>> symmetricDifference = MapUtils.symmetricDifference(v1, v2);
        assertThat(symmetricDifference.keySet()).containsExactly(KEY_1);
        assertThat(symmetricDifference.get(KEY_1)).isNotNull().satisfies(pair -> {
            assertThat(pair.left).isEmpty();
            assertThat(pair.right).isEmpty();
        });
    }

    @Test
    public void symmetricDifference_appendedValuesAreIncludedInSet2()
    {
        putAll(v1, KEY_1, TOKEN_1);
        putAll(v2, KEY_1, TOKEN_1, TOKEN_2);
        Map<String, Pair<Set<Token>, Set<Token>>> symmetricDifference = MapUtils.symmetricDifference(v1, v2);
        assertThat(symmetricDifference.keySet()).containsExactly(KEY_1);
        assertThat(symmetricDifference.get(KEY_1)).isNotNull().satisfies(pair -> {
            assertThat(pair.left).isEmpty();
            assertThat(pair.right).containsExactlyInAnyOrder(TOKEN_2);
        });
    }

    @Test
    public void symmetricDifference_remainingValuesAreIncludedInSet1()
    {
        putAll(v1, KEY_1, TOKEN_1, TOKEN_2);
        putAll(v2, KEY_1, TOKEN_1);
        Map<String, Pair<Set<Token>, Set<Token>>> symmetricDifference = MapUtils.symmetricDifference(v1, v2);
        assertThat(symmetricDifference.keySet()).containsExactly(KEY_1);
        assertThat(symmetricDifference.get(KEY_1)).isNotNull().satisfies(pair -> {
            assertThat(pair.left).containsExactlyInAnyOrder(TOKEN_2);
            assertThat(pair.right).isEmpty();
        });
    }

    @Test
    public void intersection_noIntersectionReturnsEmptySet()
    {
        putAll(addressRange, rangeOf("0", "8"), ep1);
        assertThat(MapUtils.intersection(addressRange, rangesOf(rangeOf("10", "12")))).isEmpty();
    }

    @Test
    public void intersection_overlappingIntervalReturnsNonEmptySet()
    {
        putAll(addressRange, rangeOf("0", "10"), ep1);
        putAll(addressRange, rangeOf("15", "30"), ep2);
        assertThat(MapUtils.intersection(addressRange, rangesOf(rangeOf("4", "18"))))
                .containsExactlyInAnyOrder(ep1, ep2);
    }

    @Test
    public void coalesce_sortRanges()
    {
        PendingRangeMaps pendingRangeMaps = new PendingRangeMaps();
        Range<Token> range1 = rangeOf("50", "60");
        Range<Token> range2 = rangeOf("10", "20");
        Range<Token> range3 = rangeOf("30", "32");
        pendingRangeMaps.addPendingRange(range1, ep1);
        pendingRangeMaps.addPendingRange(range2, ep1);
        pendingRangeMaps.addPendingRange(range3, ep1);
        assertThat(MapUtils.coalesce(pendingRangeMaps).keySet()).containsExactly(ep1);
        assertThat(MapUtils.coalesce(pendingRangeMaps).get(ep1))
                .isNotNull()
                .containsExactly(range2, range3, range1);
    }

    @SafeVarargs
    private final <U, V> void putAll(Multimap<U, V> map, U key, V... tokens)
    {
        map.putAll(key, Arrays.asList(tokens));
    }

    @SafeVarargs
    private final List<Range<Token>> rangesOf(Range<Token>... ranges)
    {
        return new ArrayList<>(Arrays.asList(ranges));
    }

    private Range<Token> rangeOf(String leftBound, String rightBound)
    {
        return new Range<>(new RandomPartitioner.BigIntegerToken(leftBound), new RandomPartitioner.BigIntegerToken(rightBound));
    }
}
