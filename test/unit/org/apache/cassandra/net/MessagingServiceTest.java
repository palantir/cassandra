/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.net;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import com.palantir.cassandra.ppam.InetAddressHostname;
import com.palantir.cassandra.ppam.InetAddressIp;
import com.palantir.cassandra.ppam.PrivatePublicAddressMappingAck;
import com.palantir.cassandra.ppam.PrivatePublicAddressMappingSyn;

import static org.junit.Assert.assertEquals;

public class MessagingServiceTest
{
    private final MessagingService messagingService = MessagingService.test();

    @Test
    public void testDroppedMessages()
    {
        MessagingService.Verb verb = MessagingService.Verb.READ;

        for (int i = 0; i < 5000; i++)
            messagingService.incrementDroppedMessages(verb, i % 2 == 0);

        List<String> logs = messagingService.getDroppedMessagesLogs();
        assertEquals(1, logs.size());
        assertEquals("READ messages were dropped in last 5000 ms: 2500 for internal timeout and 2500 for cross node timeout", logs.get(0));
        assertEquals(5000, (int)messagingService.getDroppedMessages().get(verb.toString()));

        logs = messagingService.getDroppedMessagesLogs();
        assertEquals(0, logs.size());

        for (int i = 0; i < 2500; i++)
            messagingService.incrementDroppedMessages(verb, i % 2 == 0);

        logs = messagingService.getDroppedMessagesLogs();
        assertEquals("READ messages were dropped in last 5000 ms: 1250 for internal timeout and 1250 for cross node timeout", logs.get(0));
        assertEquals(7500, (int)messagingService.getDroppedMessages().get(verb.toString()));
    }

    @Test
    public void receive_handlesPPAMSyn() throws UnknownHostException
    {
        InetAddressHostname sourceName = new InetAddressHostname("source");
        InetAddressIp sourceInternalIp = new InetAddressIp("1.0.0.0");
        InetAddressHostname targetName = new InetAddressHostname("target");
        InetAddressIp targetExternalIp = new InetAddressIp("2.0.0.0");
        InetAddress remote = InetAddress.getByName("127.0.0.2");
        MessageIn<PrivatePublicAddressMappingSyn> messageIn = MessageIn.create(
            remote,
            new PrivatePublicAddressMappingSyn(sourceName, sourceInternalIp, targetName, targetExternalIp),
            Collections.emptyMap(),
            MessagingService.Verb.PRIVATE_PUBLIC_ADDR_MAPPING_SYN,
            MessagingService.current_version);
        messagingService.receive(messageIn, 0, 0, false);
    }

    @Test
    public void receive_handlesPPAMAck() throws UnknownHostException
    {
        InetAddress remote = InetAddress.getByName("127.0.0.2");
        InetAddressHostname targetName = new InetAddressHostname("target");
        InetAddressIp targetExternalIp = new InetAddressIp("2.0.0.0");
        InetAddressIp targetInternalIp = new InetAddressIp("1.0.0.0");
        PrivatePublicAddressMappingAck ack = new PrivatePublicAddressMappingAck(targetName, targetInternalIp, targetExternalIp);

        MessageIn<PrivatePublicAddressMappingAck> messageIn = MessageIn.create(
            remote,
            ack,
            Collections.emptyMap(),
            MessagingService.Verb.PRIVATE_PUBLIC_ADDR_MAPPING_ACK,
            MessagingService.current_version);
        messagingService.receive(messageIn, 0, 0, false);
    }
}
