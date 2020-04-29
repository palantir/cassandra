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
package org.apache.cassandra.locator;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.FBUtilities;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleSeedProvider implements SeedProvider
{
    private static final Logger logger = LoggerFactory.getLogger(SimpleSeedProvider.class);

    // If this flag is not set to true, then node is required to attempt a bootstrap
    private final boolean isNewCluster;

    public SimpleSeedProvider(Map<String, String> args)
    {
        isNewCluster = Boolean.getBoolean("palantir_cassandra.is_new_cluster");
    }

    @VisibleForTesting
    public SimpleSeedProvider(boolean isNewCluster)
    {
        this.isNewCluster = isNewCluster;
    }

    public List<InetAddress> getSeeds()
    {
        Config conf;
        try
        {
            conf = DatabaseDescriptor.loadConfig();
        }
        catch (Exception e)
        {
            throw new AssertionError(e);
        }
        String[] hosts = conf.seed_provider.parameters.get("seeds").split(",", -1);
        InetAddress self = FBUtilities.getBroadcastAddress();
        return getSeeds(hosts, self);
    }

    @VisibleForTesting
    List<InetAddress> getSeeds(String[] hosts, InetAddress self) {
        // Add all seeds except this node
        List<InetAddress> seeds = new ArrayList<InetAddress>(hosts.length);
        for (String host : hosts)
        {
            try
            {
                InetAddress seed = InetAddress.getByName(host.trim());
                if (!self.equals(seed))
                {
                    seeds.add(seed);
                }
            }
            catch (UnknownHostException ex)
            {
                // not fatal... DD will bark if there end up being zero seeds.
                logger.warn("Seed provider couldn't lookup host {}", host);
            }
        }

        if (seeds.isEmpty() && isNewCluster)
        {
            seeds.add(self);
        }

        return Collections.unmodifiableList(seeds);
    }
}
