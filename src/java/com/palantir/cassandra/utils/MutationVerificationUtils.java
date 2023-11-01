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
import java.util.Collection;
import java.util.List;

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
    private static final boolean VERIFY_KEYS_ON_WRITE = Boolean.valueOf(System.getProperty("palantir_cassandra.verify_keys_on_write", "false"));
    private static final Logger logger = LoggerFactory.getLogger(MutationVerificationUtils.class);

    private MutationVerificationUtils() {}

    public static void verifyMutation(Mutation mutation) {
        if (!VERIFY_KEYS_ON_WRITE) {
            return;
        }
        Keyspace keyspace = Keyspace.open(mutation.getKeyspaceName());
        if (keyspace.getReplicationStrategy() instanceof NetworkTopologyStrategy) {
            Token tk = StorageService.getPartitioner().getToken(mutation.key());
            List<InetAddress> naturalEndpoints = StorageService.instance.getNaturalEndpoints(mutation.getKeyspaceName(), tk);
            Collection<InetAddress> pendingEndpoints = StorageService.instance.getTokenMetadata().pendingEndpointsFor(tk, mutation.getKeyspaceName());

            if(!naturalEndpoints.contains(FBUtilities.getBroadcastAddress()) && !pendingEndpoints.contains(FBUtilities.getBroadcastAddress())) {
                keyspace.metric.invalidMutations.inc();
                logger.error("Cannot apply mutation as this host {} does not contain key {} in keyspace {}. Only hosts {} and {} do.",
                             FBUtilities.getBroadcastAddress(),
                             Hex.bytesToHex(mutation.key().array()),
                             mutation.getKeyspaceName(),
                             naturalEndpoints,
                             pendingEndpoints);
                throw new RuntimeException("Cannot apply mutation as this host does not contain key.");
            }

            keyspace.metric.validMutations.inc();
        }
    }

}
