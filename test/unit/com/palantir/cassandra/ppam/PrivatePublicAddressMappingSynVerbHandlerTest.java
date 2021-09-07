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
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import com.google.common.net.InetAddresses;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.assertj.core.api.Assertions;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

public class PrivatePublicAddressMappingSynVerbHandlerTest
{
    private final PrivatePublicAddressMappingSynVerbHandler handler = spy(new PrivatePublicAddressMappingSynVerbHandler());

    @Before
    public void before() throws UnknownHostException
    {
        doNothing().when(handler).reply(any(), any());
    }

    @Test
    public void doVerb_updatesNewMapping() throws UnknownHostException
    {
        InetAddress remote = InetAddress.getByName("127.0.0.2");
        InetAddressHostname sourceName = new InetAddressHostname("localhost");
        InetAddressIp sourceExternalIp = new InetAddressIp("127.0.0.1");
        InetAddressIp sourceInternalIp = new InetAddressIp("1.0.0.0");
        InetAddressHostname targetName = new InetAddressHostname("target");
        InetAddressIp targetExternalIp = new InetAddressIp("2.0.0.0");
        PrivatePublicAddressMappingSyn syn = new PrivatePublicAddressMappingSyn(sourceName, sourceInternalIp, targetName, targetExternalIp);

        MessageIn<PrivatePublicAddressMappingSyn> messageIn = MessageIn.create(
            remote,
            syn,
            Collections.emptyMap(),
            MessagingService.Verb.PRIVATE_PUBLIC_ADDR_MAPPING_SYN,
            MessagingService.current_version);

        PrivatePublicAddressMappingCoordinator.instance.clearPrivatePublicAddressMapping();
        Map<InetAddressIp, InetAddressIp> map = PrivatePublicAddressMappingCoordinator.instance.getPrivatePublicAddressMapping();
        Assertions.assertThat(map).hasSize(0);
        handler.doVerb(messageIn, 0);
        Assertions.assertThat(map).hasSize(1);

        Assertions.assertThat(map.get(sourceInternalIp)).isEqualTo(sourceExternalIp);
    }

    @Test
    public void doVerb_updatesPreExistingMapping()
    {
    }

    @Test
    public void doVerb_sendsAckToSourceExternal()
    {
    }
}
