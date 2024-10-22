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
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Hex;

public class OwnershipVerificationUtils
{
    private static final boolean VERIFY_KEYS_ON_WRITE = Boolean.getBoolean("palantir_cassandra.verify_keys_on_write");
    private static final boolean VERIFY_KEYS_ON_READ = Boolean.getBoolean("palantir_cassandra.verify_keys_on_read");
    private static final Logger logger = LoggerFactory.getLogger(OwnershipVerificationUtils.class);

    private static volatile Instant lastTokenRingCacheUpdate = Instant.MIN;

    private OwnershipVerificationUtils()
    {
    }

    public static void verifyRead(Keyspace keyspace, ByteBuffer key)
    {
        if (!VERIFY_KEYS_ON_READ)
        {
            return;
        }
        verifyOperation(keyspace, key, ReadViolationHandler.INSTANCE);
    }

    public static void verifyMutation(Mutation mutation)
    {
        if (!VERIFY_KEYS_ON_WRITE)
        {
            return;
        }
        verifyOperation(Keyspace.open(mutation.getKeyspaceName()), mutation.key(), MutationViolationHandler.INSTANCE);
    }

    private static void verifyOperation(Keyspace keyspace, ByteBuffer key, OwnershipViolationHandler handler)
    {
        if (!(keyspace.getReplicationStrategy() instanceof NetworkTopologyStrategy))
        {
            return;
        }

        String keyspaceName = keyspace.getName();
        Token tk = StorageService.getPartitioner().getToken(key);
        List<InetAddress> cachedNaturalEndpoints = StorageService.instance.getNaturalEndpoints(keyspaceName, tk);
        Collection<InetAddress> pendingEndpoints = StorageService.instance.getTokenMetadata().pendingEndpointsFor(tk, keyspaceName);

        if (operationIsInvalid(cachedNaturalEndpoints, pendingEndpoints))
        {
            if (cacheWasRecentlyRefreshed())
            {
                handler.onViolation(keyspace, key, cachedNaturalEndpoints, pendingEndpoints);
            }

            refreshCache();

            List<InetAddress> refreshedNaturalEndpoints = StorageService.instance.getNaturalEndpoints(keyspaceName, tk);

            if (operationIsInvalid(refreshedNaturalEndpoints, pendingEndpoints))
            {
                handler.onViolation(keyspace, key, cachedNaturalEndpoints, pendingEndpoints);
            }
            else
            {
                logger.warn("Ignoring InvalidOwnership error detected using stale token ring cache. Error was originally detected for key {} in keyspace {}."
                                + " Cached owners {}. Actual owners {}. Pending owners (non-cached) {}.",
                            UnsafeArg.of("key", Hex.bytesToHex(key.array())),
                            SafeArg.of("keyspace", keyspaceName),
                            SafeArg.of("cachedNaturalEndpoints", cachedNaturalEndpoints),
                            SafeArg.of("refreshedNaturalEndpoints", refreshedNaturalEndpoints),
                            SafeArg.of("pendingEndpoints", pendingEndpoints));
            }
        }
        handler.onValid(keyspace);
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

    private static boolean operationIsInvalid(List<InetAddress> naturalEndpoints, Collection<InetAddress> pendingEndpoints)
    {
        return !naturalEndpoints.contains(FBUtilities.getBroadcastAddress()) && !pendingEndpoints.contains(FBUtilities.getBroadcastAddress());
    }

    @VisibleForTesting
    static void clearLastTokenRingCacheUpdate()
    {
        lastTokenRingCacheUpdate = Instant.MIN;
    }
}
