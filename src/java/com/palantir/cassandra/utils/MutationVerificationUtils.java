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
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import org.apache.cassandra.exceptions.InvalidMutationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Hex;

public class MutationVerificationUtils
{
    private static final boolean VERIFY_KEYS_ON_WRITE = Boolean.getBoolean("palantir_cassandra.verify_keys_on_write");
    private static final Logger logger = LoggerFactory.getLogger(MutationVerificationUtils.class);

    private static volatile Instant lastTokenRingCacheUpdate = Instant.MIN;

    private MutationVerificationUtils()
    {
    }

    public static void verifyMutation(Mutation mutation)
    {
        if (!VERIFY_KEYS_ON_WRITE)
        {
            return;
        }

        Keyspace keyspace = Keyspace.open(mutation.getKeyspaceName());
        if (!(keyspace.getReplicationStrategy() instanceof NetworkTopologyStrategy))
        {
            return;
        }

        Token tk = StorageService.getPartitioner().getToken(mutation.key());
        List<InetAddress> cachedNaturalEndpoints = StorageService.instance.getNaturalEndpoints(mutation.getKeyspaceName(), tk);
        Collection<InetAddress> pendingEndpoints = StorageService.instance.getTokenMetadata().pendingEndpointsFor(tk, mutation.getKeyspaceName());

        if (mutationIsInvalid(cachedNaturalEndpoints, pendingEndpoints))
        {
            if (cacheWasRecentlyRefreshed())
            {
                throwInvalidMutationException(mutation, keyspace, cachedNaturalEndpoints, pendingEndpoints);
            }

            refreshCache();

            List<InetAddress> refreshedNaturalEndpoints = StorageService.instance.getNaturalEndpoints(mutation.getKeyspaceName(), tk);

            if (mutationIsInvalid(refreshedNaturalEndpoints, pendingEndpoints))
            {
                throwInvalidMutationException(mutation, keyspace, refreshedNaturalEndpoints, pendingEndpoints);
            }
            else
            {
                logger.warn("Ignoring InvalidMutation error detected using stale token ring cache. Error was originally detected for key {} in keyspace {}."
                                + " Cached owners {} and {}. Actual owners {} and {}",
                        Hex.bytesToHex(mutation.key().array()),
                        mutation.getKeyspaceName(),
                        cachedNaturalEndpoints,
                        pendingEndpoints,
                        refreshedNaturalEndpoints,
                        pendingEndpoints);
            }
        }

        keyspace.metric.validMutations.inc();
    }


    private static void throwInvalidMutationException(Mutation mutation, Keyspace keyspace, List<InetAddress> naturalEndpoints, Collection<InetAddress> pendingEndpoints)
    {
        keyspace.metric.invalidMutations.inc();
        logger.error("InvalidMutation! Cannot apply mutation as this host {} does not contain key {} in keyspace {}. Only hosts {} and {} do.",
                FBUtilities.getBroadcastAddress(), Hex.bytesToHex(mutation.key().array()), mutation.getKeyspaceName(), naturalEndpoints, pendingEndpoints);
        throw new InvalidMutationException();
    }

    private static void refreshCache()
    {
        StorageService.instance.getTokenMetadata().invalidateCachedRings();
        lastTokenRingCacheUpdate = Instant.now();
    }

    private static boolean cacheWasRecentlyRefreshed()
    {
        return Duration.between(lastTokenRingCacheUpdate, Instant.now()).compareTo(Duration.ofMinutes(10)) < 0;
    }

    private static boolean mutationIsInvalid(List<InetAddress> naturalEndpoints, Collection<InetAddress> pendingEndpoints)
    {
        return !naturalEndpoints.contains(FBUtilities.getBroadcastAddress()) && !pendingEndpoints.contains(FBUtilities.getBroadcastAddress());
    }

    @VisibleForTesting
    static void clearLastTokenRingCacheUpdate()
    {
        lastTokenRingCacheUpdate = Instant.MIN;
    }

}
