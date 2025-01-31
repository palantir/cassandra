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

import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import org.apache.cassandra.db.AbstractRangeCommand;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.utils.FBUtilities;

public class RangeSliceVerificationHandler implements OwnershipVerificationHandler<AbstractRangeCommand>
{
    private static final Logger logger = LoggerFactory.getLogger(OwnershipVerificationUtils.class);

    public static final OwnershipVerificationHandler<AbstractRangeCommand> INSTANCE = new RangeSliceVerificationHandler();

    @Override
    public void onViolation(AbstractRangeCommand command, Keyspace keyspace, List<InetAddress> naturalEndpoints, Collection<InetAddress> pendingEndpoints)
    {
        keyspace.metric.invalidRangeSlice.inc();
        logger.error(
        "Received Invalid RangeSlice request! This host {} does not contain range ({}, {}) in keyspace {}. Only hosts {} and {} do.",
            SafeArg.of("address", FBUtilities.getBroadcastAddress()),
            UnsafeArg.of("left", command.keyRange.left),
            UnsafeArg.of("right", command.keyRange.right),
            SafeArg.of("keyspace", keyspace.getName()),
            SafeArg.of("naturalEndpoints", naturalEndpoints),
            SafeArg.of("pendingEndpoints", pendingEndpoints));
        throw new RuntimeException("InvalidRangeSlice! Cannot serve this range slice as this host does not contain the right bound of this range.");
    }

    @Override
    public void onValid(Keyspace keyspace)
    {
        keyspace.metric.validRangeSlice.inc();
    }
}
