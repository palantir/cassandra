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
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class CrossVpcIpMappingAckVerbHandlerTest
{
    private final CrossVpcIpMappingAckVerbHandler handler = spy(new CrossVpcIpMappingAckVerbHandler());

    @Test
    public void doVerb_invokedByMessagingService() throws UnknownHostException
    {
        InetAddress remote = InetAddress.getByName("127.0.0.2");
        InetAddressHostname targetName = new InetAddressHostname("target");
        InetAddressIp targetExternalIp = new InetAddressIp("2.0.0.0");
        InetAddressIp targetInternalIp = new InetAddressIp("1.0.0.0");
        CrossVpcIpMappingAck ack = new CrossVpcIpMappingAck(targetName, targetInternalIp, targetExternalIp);

        MessageIn<CrossVpcIpMappingAck> messageIn = MessageIn.create(remote,
                                                                     ack,
                                                                     Collections.emptyMap(),
                                                                     MessagingService.Verb.CROSS_VPC_IP_MAPPING_ACK,
                                                                     MessagingService.current_version);

        MessagingService.instance().registerVerbHandlers(MessagingService.Verb.CROSS_VPC_IP_MAPPING_ACK, handler);
        MessagingService.instance().receive(messageIn, 0, 0, false);
        // Potential race condition since MessageDeliveryTask is run in another executor
        verify(handler, times(1)).doVerb(eq(messageIn), anyInt());
    }

    @Test
    public void doVerb_invokesCrossVpcIpMappingHandshaker() throws UnknownHostException
    {
        InetAddress remote = InetAddress.getByName("127.0.0.2");
        InetAddressHostname targetName = new InetAddressHostname("target");
        InetAddressIp targetExternalIp = new InetAddressIp("2.2.2.2");
        InetAddressIp targetInternalIp = new InetAddressIp("127.0.0.1");
        InetAddress input = InetAddress.getByName(targetInternalIp.toString());
        CrossVpcIpMappingAck ack = new CrossVpcIpMappingAck(targetName, targetInternalIp, targetExternalIp);
        MessageIn<CrossVpcIpMappingAck> messageIn = MessageIn.create(remote,
                                                                     ack,
                                                                     Collections.emptyMap(),
                                                                     MessagingService.Verb.CROSS_VPC_IP_MAPPING_ACK,
                                                                     MessagingService.current_version);
        DatabaseDescriptor.setCrossVpcHostnameSwapping(false);
        DatabaseDescriptor.setCrossVpcIpSwapping(true);
        InetAddress result = CrossVpcIpMappingHandshaker.instance.maybeSwapAddress(input);
        assertThat(result.getHostAddress()).isNotEqualTo(targetExternalIp.toString());
        handler.doVerb(messageIn, 0);
        result = CrossVpcIpMappingHandshaker.instance.maybeSwapAddress(input);
        assertThat(result.getHostAddress()).isEqualTo(targetExternalIp.toString());
    }
}
