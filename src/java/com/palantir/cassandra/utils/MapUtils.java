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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.PendingRangeMaps;
import org.apache.cassandra.utils.BiMultiValMap;
import org.apache.cassandra.utils.Pair;


public final class MapUtils
{
    private MapUtils()
    {
    }

    /**
     * For each key in the union of all keys, find their symmetric difference between the versions of the map.
     * For a given key, {@link Pair#left} are values that exist only in the first map and {@link Pair#right} are values
     * that exist only in the second map.
     */
    public static <U, V> Map<U, Pair<Collection<V>, Collection<V>>> symmetricDifference(Multimap<U, V> v1, Multimap<U, V> v2)
    {
        Set<U> keys = Sets.union(v1.keySet(), v2.keySet()).immutableCopy();
        Map<U, Pair<Collection<V>, Collection<V>>> symmetricDifference = new HashMap<>();

        for (U key : keys)
        {
            Set<V> valuesFromV1 = new HashSet<>(v1.get(key));
            Set<V> valuesFromV2 = new HashSet<>(v2.get(key));
            symmetricDifference.put(key, Pair.create(Sets.difference(valuesFromV1, valuesFromV2), Sets.difference(valuesFromV2, valuesFromV1)));
        }

        return symmetricDifference;
    }

    /**
     * Returns a list of endpoints where its token range intersect with any token ranges in the input list.
     */
    public static Set<InetAddress> intersection(Multimap<Range<Token>, InetAddress> addressRanges, Collection<Range<Token>> tokenRanges)
    {
        Set<InetAddress> intersection = new HashSet<>();

        for (Map.Entry<Range<Token>, InetAddress> entry : addressRanges.entries())
        {
            for (Range<Token> range : tokenRanges)
            {
                if (entry.getKey().intersects(range))
                {
                    intersection.add(entry.getValue());
                }
            }
        }

        return intersection;
    }
}
