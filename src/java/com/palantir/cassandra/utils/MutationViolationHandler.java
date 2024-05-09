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

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.InvalidOwnershipException;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;

public class MutationViolationHandler implements OwnershipViolationHandler
{
    private static final Logger logger = LoggerFactory.getLogger(OwnershipVerificationUtils.class);

    public static final OwnershipViolationHandler INSTANCE = new MutationViolationHandler();
    @Override
    public void onViolation(Keyspace keyspace, ByteBuffer key, List<InetAddress> naturalEndpoints, Collection<InetAddress> pendingEndpoints)
    {
        keyspace.metric.invalidMutations.inc();
        logger.error("InvalidMutation! Cannot apply mutation as this host {} does not contain key {} in keyspace {}. Only hosts {} and {} do.",
                FBUtilities.getBroadcastAddress(), Hex.bytesToHex(key.array()),keyspace.getName(), naturalEndpoints, pendingEndpoints);
        throw new InvalidOwnershipException();
    }

    @Override
    public void onValid(Keyspace keyspace)
    {
        keyspace.metric.validMutations.inc();
    }
}
