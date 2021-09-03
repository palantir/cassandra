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


import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;

public class PrivatePublicAddressMappingSynVerbHandler implements IVerbHandler<PrivatePublicAddressMappingSyn>
{
    private static final Logger logger = LoggerFactory.getLogger(PrivatePublicAddressMappingSynVerbHandler.class);
    public void doVerb(MessageIn<PrivatePublicAddressMappingSyn> message, int id) throws UnknownHostException
    {
        PrivatePublicAddressMappingSyn synMessage = message.payload;
        logger.trace("Handling new PPAM Syn message from {}/{}", message.from, synMessage.getSourceHostname());

        InetAddressHostname sourceName = synMessage.getSourceHostname();
        InetAddressIp sourceInternalIp = synMessage.getSourceInternalAddress();

        InetAddressHostname targetName = synMessage.getTargetHostname();
        InetAddressIp targetExternalIp = synMessage.getTargetExternalAddress();

        // InetAddress.getByHostname performs a DNS lookup
        InetAddressIp sourceExternalIp = new InetAddressIp(InetAddress.getByName(sourceName.toString()).getHostAddress());

        InetAddressIp targetInternalIp = new InetAddressIp(FBUtilities.getBroadcastAddress().getHostAddress());

        PrivatePublicAddressMappingCoordinator.instance.updatePrivatePublicAddressMapping(sourceName, sourceInternalIp, sourceExternalIp);

        PrivatePublicAddressMappingAck ack = new PrivatePublicAddressMappingAck(targetName, targetInternalIp, targetExternalIp);
        MessageOut<PrivatePublicAddressMappingAck> ackMessage = new MessageOut<>(
                                                    MessagingService.Verb.PRIVATE_PUBLIC_ADDR_MAPPING_ACK,
                                                    ack,
                                                    PrivatePublicAddressMappingAck.serializer);
        logger.trace("Sending PrivatePublicAddressMappingAck to {}", sourceExternalIp);
        reply(ackMessage, sourceExternalIp);
    }

    @VisibleForTesting
    void reply(MessageOut<PrivatePublicAddressMappingAck> ackMessage, InetAddressIp sourceExternal) throws UnknownHostException
    {
        MessagingService.instance().sendOneWay(ackMessage, InetAddress.getByName(sourceExternal.toString()));
    }
}
