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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
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

    private Multimap<String, Token> v1;

    private Multimap<String, Token> v2;

    @Before
    public void before()
    {
        v1 = HashMultimap.<String, Token>create();
        v2 = HashMultimap.<String, Token>create();
    }

    @Test
    public void symmetricDifference_sameMapContentHasEmptySet()
    {
        putAll(v1, KEY_1, TOKEN_1);
        putAll(v2, KEY_1, TOKEN_1);
        Map<String, Pair<Collection<Token>, Collection<Token>>> symmetricDifference = MapUtils.symmetricDifference(v1, v2);
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
        Map<String, Pair<Collection<Token>, Collection<Token>>> symmetricDifference = MapUtils.symmetricDifference(v1, v2);
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
        Map<String, Pair<Collection<Token>, Collection<Token>>> symmetricDifference = MapUtils.symmetricDifference(v1, v2);
        assertThat(symmetricDifference.keySet()).containsExactly(KEY_1);
        assertThat(symmetricDifference.get(KEY_1)).isNotNull().satisfies(pair -> {
            assertThat(pair.left).containsExactlyInAnyOrder(TOKEN_2);
            assertThat(pair.right).isEmpty();
        });
    }

    private void putAll(Multimap<String, Token> map, String key, Token... tokens)
    {
        map.putAll(key, Arrays.asList(tokens));
    }
}
