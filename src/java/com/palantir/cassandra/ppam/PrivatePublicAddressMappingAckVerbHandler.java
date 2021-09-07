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

package com.palantir.cassandra.ppam;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;

public class PrivatePublicAddressMappingAckVerbHandler implements IVerbHandler<PrivatePublicAddressMappingAck>
{
    private static final Logger logger = LoggerFactory.getLogger(PrivatePublicAddressMappingAckVerbHandler.class);
    public void doVerb(MessageIn<PrivatePublicAddressMappingAck> message, int id)
    {
        PrivatePublicAddressMappingAck ackMessage = message.payload;
        InetAddressHostname targetName = ackMessage.getTargetHostname();
        logger.trace("Handling new PPAM Ack message from {}/{}", targetName, message.from);

        InetAddressIp targetInternalIp = ackMessage.getTargetInternalAddress();
        InetAddressIp targetExternalIp = ackMessage.getTargetExternalAddress();

        PrivatePublicAddressMappingCoordinator.instance.updatePrivatePublicAddressMapping(targetName, targetInternalIp, targetExternalIp);
    }
}
