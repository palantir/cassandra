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

package com.palantir.cassandra.cvim;

import java.net.InetAddress;
import java.net.UnknownHostException;


import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.security.SSLFactory;
import org.apache.cassandra.utils.FBUtilities;

public class CrossVpcIpMappingSynVerbHandler implements IVerbHandler<CrossVpcIpMappingSyn>
{
    private static final Logger logger = LoggerFactory.getLogger(CrossVpcIpMappingSynVerbHandler.class);

    public void doVerb(MessageIn<CrossVpcIpMappingSyn> message, int id) throws UnknownHostException
    {
        CrossVpcIpMappingSyn synMessage = message.payload;
        InetAddressHostname sourceName = synMessage.getSourceHostname();
        InetAddressIp sourceInternalIp = synMessage.getSourceInternalAddress();

        InetAddressHostname proposedTargetName = synMessage.getTargetHostname();
        InetAddressIp proposedTargetExternalIp = synMessage.getTargetExternalAddress();

        // InetAddress.getByHostname performs a DNS lookup
        InetAddressIp sourceExternalIp = new InetAddressIp(InetAddress.getByName(sourceName.toString())
                                                                      .getHostAddress());

        InetAddress broadcastAddress = FBUtilities.getBroadcastAddress();
        InetAddressIp targetInternalIp = new InetAddressIp(broadcastAddress.getHostAddress());

        if (!DatabaseDescriptor.isCrossVpcInternodeCommunicationEnabled())
        {
            logger.trace("Ignoring new Cross-VPC-IP-Mapping Syn message from {}. source: {}/{} -> {}; target: {} -> {} " +
                         "because cross_vpc_internode_communication_enabled=false",
                         message.from,
                         synMessage.getSourceHostname(),
                         sourceInternalIp,
                         sourceExternalIp,
                         targetInternalIp,
                         proposedTargetExternalIp);
            return;
        }

        if (!proposedTargetName.equals(getBroadcastHostname()))
        {
            logger.warn("Received a cross VPC Syn intended for another host. Ignoring. Intended: {} Actual: {}",
                        proposedTargetName, broadcastAddress);
            return;
        }

        logger.trace("Handling new Cross-VPC-IP-Mapping Syn message from {}. source: {}/{} -> {}; target: {} -> {}",
                     message.from,
                     synMessage.getSourceHostname(),
                     sourceInternalIp,
                     sourceExternalIp,
                     targetInternalIp,
                     proposedTargetExternalIp);


        CrossVpcIpMappingHandshaker.instance.updateCrossVpcMappings(sourceName, sourceInternalIp);

        CrossVpcIpMappingAck ack = new CrossVpcIpMappingAck(proposedTargetName, targetInternalIp, proposedTargetExternalIp);
        MessageOut<CrossVpcIpMappingAck> ackMessage = new MessageOut<>(MessagingService.Verb.CROSS_VPC_IP_MAPPING_ACK,
                                                                       ack,
                                                                       CrossVpcIpMappingAck.serializer);

        logger.trace("Sending CrossVpcIpMappingAck to {}/{}", sourceInternalIp, sourceName);
        reply(ackMessage, sourceInternalIp);
    }

    @VisibleForTesting
    InetAddressHostname getBroadcastHostname() {
        // Supplying broadcast address as a hostname via config avoids a reverse-DNS lookup
        return new InetAddressHostname(FBUtilities.getBroadcastAddress().getHostName());
    }

    /**
     * Send to sourceInternal because {@link CrossVpcIpMappingHandshaker} should now contain the mapping with which
     * {@link SSLFactory} will use to swap out the internal IP for public.
     */
    @VisibleForTesting
    void reply(MessageOut<CrossVpcIpMappingAck> ackMessage, InetAddressIp sourceInternal) throws UnknownHostException
    {
        MessagingService.instance().sendOneWay(ackMessage, InetAddress.getByName(sourceInternal.toString()));
    }
}
