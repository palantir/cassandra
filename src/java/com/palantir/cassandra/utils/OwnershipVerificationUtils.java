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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.cassandra.db.TokenOwnershipSnapshot;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RangeSliceCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;
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

    public static void verifyRead(Keyspace keyspace, MessageIn<ReadCommand> message)
    {
        if (!VERIFY_KEYS_ON_READ)
        {
            return;
        }
        Set<InetAddress> reportedOwners = TokenOwnershipSnapshot.deserialize(message.parameters.get(MessagingService.TOKEN_OWNERS_PARAM));

        ByteBuffer key = message.payload.key;
        verifyOperation(keyspace, key, reportedOwners, ReadVerificationHandler.INSTANCE);
    }

    public static void verifyMutation(MessageIn<Mutation> message)
    {
        if (!VERIFY_KEYS_ON_WRITE)
        {
            return;
        }
        Set<InetAddress> reportedOwners = TokenOwnershipSnapshot.deserialize(message.parameters.get(MessagingService.TOKEN_OWNERS_PARAM));

        Mutation mutation = message.payload;
        verifyOperation(Keyspace.open(mutation.getKeyspaceName()), mutation.key(), reportedOwners, MutationVerificationHandler.INSTANCE);
    }

    private static void verifyOperation(Keyspace keyspace, ByteBuffer key, Set<InetAddress> reportedOwners, OwnershipVerificationHandler handler)
    {
        if (!(keyspace.getReplicationStrategy() instanceof NetworkTopologyStrategy))
        {
            return;
        }

        String keyspaceName = keyspace.getName();
        Token tk = StorageService.getPartitioner().getToken(key);
        List<InetAddress> cachedNaturalEndpoints = StorageService.instance.getNaturalEndpoints(keyspaceName, tk);
        Collection<InetAddress> pendingEndpoints = StorageService.instance.getTokenMetadata().pendingEndpointsFor(tk, keyspaceName);

        HashSet<InetAddress> localOwners = new HashSet<>(cachedNaturalEndpoints);
        localOwners.addAll(pendingEndpoints);

        if (operationIsInvalid(localOwners, reportedOwners))
        {
            if (cacheWasRecentlyRefreshed())
            {
                handler.onViolation(keyspace, key, cachedNaturalEndpoints, pendingEndpoints);
                return;
            }

            refreshCache();

            List<InetAddress> refreshedNaturalEndpoints = StorageService.instance.getNaturalEndpoints(keyspaceName, tk);
            localOwners.clear();
            localOwners.addAll(refreshedNaturalEndpoints);
            localOwners.addAll(pendingEndpoints);

            if (operationIsInvalid(localOwners, reportedOwners))
            {
                handler.onViolation(keyspace, key, refreshedNaturalEndpoints, pendingEndpoints);
                return;
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

    private static boolean operationIsInvalid(Set<InetAddress> localOwners, Set<InetAddress> reportedOwners)
    {
        return !localOwners.contains(FBUtilities.getBroadcastAddress()) || !localOwners.equals(reportedOwners);
    }

    @VisibleForTesting
    static void clearLastTokenRingCacheUpdate()
    {
        lastTokenRingCacheUpdate = Instant.MIN;
    }
}
