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
import java.util.stream.Collectors;

import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.PendingRangeMaps;
import org.apache.cassandra.utils.Pair;


public final class MapUtils
{
    private MapUtils()
    {
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

    /**
     * Merge the pending ranges per endpoint and return as a sorted list.
     */
    public static Map<InetAddress, List<Range<Token>>> coalesce(PendingRangeMaps pendingRangeMaps)
    {
        Map<InetAddress, List<Range<Token>>> coalesced = new HashMap<>();

        for (Map.Entry<Range<Token>, List<InetAddress>> entry : pendingRangeMaps)
        {
            for (InetAddress endpoint : entry.getValue())
            {
                coalesced.computeIfAbsent(endpoint, _k -> new ArrayList<>()).add(entry.getKey());
            }
        }
        coalesced.replaceAll((_k, tokenRanges) -> sort(tokenRanges));

        return coalesced;
    }

    /**
     * Similar to {@link MapUtils#coalesce(Multimap)}
     */
    public static Map<InetAddress, List<Token>> coalesce(Multimap<InetAddress, Token> endpointToken)
    {
        Map<InetAddress, List<Token>> coalesced = new HashMap<>();

        for (InetAddress endpoint : endpointToken.keys())
        {
            coalesced.put(endpoint, sort(endpointToken.get(endpoint)));
        }

        return coalesced;
    }

    private static <T> List<T> sort(Collection<T> collection) {
        return collection.stream().sorted().collect(Collectors.toList());
    }
}
