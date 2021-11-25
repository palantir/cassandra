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

import java.net.InetAddress;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.RangeStreamer;
import org.assertj.core.util.VisibleForTesting;

public class SingleRackFilter implements RangeStreamer.ISourceFilter
{

    private final Optional<String> maybeRack;

    public SingleRackFilter(Optional<String> maybeRack)
    {
        this.maybeRack = maybeRack;
    }

    public boolean shouldInclude(InetAddress endpoint)
    {
        return maybeRack.map(rack -> rack.equals(DatabaseDescriptor.getEndpointSnitch().getRack(endpoint))).orElse(true);
    }

    @VisibleForTesting
    Optional<String> getMaybeRack() {
        return maybeRack;
    }

    public static SingleRackFilter create(SetMultimap<String, String> datacenterRacks, String sourceDatacenter, String localDatacenter, String localRack)
    {
        Set<String> localRacks = new TreeSet<>(datacenterRacks.get(localDatacenter));
        Set<String> sourceRacks = new TreeSet<>(datacenterRacks.get(sourceDatacenter));
        Optional<String> maybeRack = Optional.empty();
        if (localRacks.size() == sourceRacks.size())
        {
            Iterator<String> sourceRacksIterator = sourceRacks.iterator();
            Iterator<String> localRacksIterator = localRacks.iterator();
            while (sourceRacksIterator.hasNext())
            {
                String sourceRack = sourceRacksIterator.next();
                if (localRack.equals(localRacksIterator.next()))
                {
                    maybeRack = Optional.of(sourceRack);
                    break;
                }
            }
        }

        return new SingleRackFilter(maybeRack);
    }
}
