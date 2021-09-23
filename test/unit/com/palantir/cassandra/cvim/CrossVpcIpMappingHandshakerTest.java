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
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.MessagingService;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CrossVpcIpMappingHandshakerTest
{
    @Before
    public void before()
    {
        DatabaseDescriptor.setCrossVpcInternodeCommunication(false);
        DatabaseDescriptor.setCrossVpcIpSwapping(false);
        DatabaseDescriptor.setCrossVpcHostnameSwapping(false);
        CrossVpcIpMappingHandshaker.instance.stop();
        CrossVpcIpMappingHandshaker.instance.clearMappings();
        CrossVpcIpMappingHandshaker.instance.setLastTriggeredHandshakeMillis(0);
    }

    @Test
    public void maybeSwapAddress_swapsHostnameWhenEnabled() throws UnknownHostException
    {
        InetAddress real = InetAddress.getByName("20.0.0.1");
        mockMapping("localhost", "20.0.0.1", "10.0.0.1", true, false, true);
        InetAddress result = CrossVpcIpMappingHandshaker.instance.maybeSwapAddress(real);
        assertThat(real.getHostName()).isEqualTo("20.0.0.1");
        assertThat(result.getHostName()).isEqualTo("localhost");
    }

    @Test
    public void maybeSwapAddress_noopsWhenHostnameUnresolvable() throws UnknownHostException
    {
        InetAddress input = InetAddress.getByName("localhost");
        mockMapping("unresolvable", "127.0.0.1", "10.0.0.1", true, false, true);
        InetAddress result = CrossVpcIpMappingHandshaker.instance.maybeSwapAddress(input);
        assertThat(result).isEqualTo(input);
    }

    @Test
    public void maybeSwapAddress_swapsIpWhenEnabled() throws UnknownHostException
    {
        InetAddress input = InetAddress.getByName("20.0.0.1");
        mockMapping("localhost", "20.0.0.1", "10.0.0.1", true, true, false);
        InetAddress result = CrossVpcIpMappingHandshaker.instance.maybeSwapAddress(input);
        assertThat(input.getHostAddress()).isEqualTo("20.0.0.1");
        assertThat(result.getHostAddress()).isEqualTo("10.0.0.1");
    }

    @Test
    public void maybeSwapAddress_swapsHostnameWhenBothEnabled() throws UnknownHostException
    {
        InetAddress input = InetAddress.getByName("20.0.0.1");
        mockMapping("localhost", "20.0.0.1", "10.0.0.1", true, true, true);
        InetAddress result = CrossVpcIpMappingHandshaker.instance.maybeSwapAddress(input);
        assertThat(input.getHostName()).isEqualTo("20.0.0.1");
        assertThat(result.getHostName()).isEqualTo("localhost");
    }

    @Test
    public void maybeSwapAddress_onlyTriesHostnameWhenBothEnabled() throws UnknownHostException
    {
        InetAddress input = InetAddress.getByName("20.0.0.1");
        mockMapping("unresolvable", "20.0.0.1", "10.0.0.1", true, true, true);
        InetAddress result = CrossVpcIpMappingHandshaker.instance.maybeSwapAddress(input);
        assertThat(input.getHostName()).isEqualTo("20.0.0.1");
        // Would be 10.0.0.1 if we fell back to IP swapping if hostname failed
        assertThat(result.getHostName()).isEqualTo("20.0.0.1");
    }

    @Test
    public void maybeSwapAddress_noopsWhenBothMappingsDisabled() throws UnknownHostException
    {
        InetAddress input = InetAddress.getByName("20.0.0.1");
        mockMapping("localhost", "20.0.0.1", "10.0.0.1", true, false, false);
        InetAddress result = CrossVpcIpMappingHandshaker.instance.maybeSwapAddress(input);
        assertThat(input).isEqualTo(result);
    }

    @Test
    public void maybeSwapAddress_noopsWhenCrossVpcComm_disabled() throws UnknownHostException
    {
        InetAddress input = InetAddress.getByName("20.0.0.1");
        mockMapping("localhost", "20.0.0.1", "10.0.0.1", false, true, true);
        InetAddress result = CrossVpcIpMappingHandshaker.instance.maybeSwapAddress(input);
        assertThat(input).isEqualTo(result);
    }

    @Test
    public void maybeSwapAddress_noopsOnIpResolutionFailure() throws UnknownHostException
    {
        InetAddressIp causeFail = mock(InetAddressIp.class);
        when(causeFail.toString()).thenReturn("ip-to-cause-exception");
        InetAddressHostname name = new InetAddressHostname("localhost");
        InetAddressIp internal = new InetAddressIp("127.0.0.1");
        CrossVpcIpMappingHandshaker.instance.updateCrossVpcMappings(name, internal, causeFail);
        DatabaseDescriptor.setCrossVpcHostnameSwapping(false);
        DatabaseDescriptor.setCrossVpcIpSwapping(true);

        InetAddress input = InetAddress.getByName("localhost");
        InetAddress result = CrossVpcIpMappingHandshaker.instance.maybeSwapAddress(input);

        assertThat(result).isEqualTo(input);
    }

    @Test
    public void updateCrossVpcMappings_updatesPrivatePublicIp()
    {
        InetAddressHostname name = new InetAddressHostname("host");
        InetAddressIp internal = new InetAddressIp("10.0.0.1");
        InetAddressIp external = new InetAddressIp("20.0.0.1");
        Map<InetAddressIp, InetAddressIp> mapping = CrossVpcIpMappingHandshaker.instance.getCrossVpcIpMapping();
        assertThat(mapping).isEmpty();
        DatabaseDescriptor.setCrossVpcInternodeCommunication(true);
        CrossVpcIpMappingHandshaker.instance.updateCrossVpcMappings(name, internal, external);
        assertThat(mapping).hasSize(1);
        assertThat(mapping).containsEntry(internal, external);
    }

    @Test
    public void updateCrossVpcMappings_updatesPrivateIpHostname()
    {
        InetAddressHostname name = new InetAddressHostname("host");
        InetAddressIp internal = new InetAddressIp("10.0.0.1");
        InetAddressIp external = new InetAddressIp("20.0.0.1");
        Map<InetAddressIp, InetAddressHostname> mapping = CrossVpcIpMappingHandshaker.instance.getCrossVpcIpHostnameMapping();
        assertThat(mapping).isEmpty();
        DatabaseDescriptor.setCrossVpcInternodeCommunication(true);
        CrossVpcIpMappingHandshaker.instance.updateCrossVpcMappings(name, internal, external);
        assertThat(mapping).hasSize(2);
        assertThat(mapping).containsEntry(internal, name);
    }

    @Test
    public void updateCrossVpcMappings_updatesPublicIpHostname()
    {
        InetAddressHostname name = new InetAddressHostname("host");
        InetAddressIp internal = new InetAddressIp("10.0.0.1");
        InetAddressIp external = new InetAddressIp("20.0.0.1");
        Map<InetAddressIp, InetAddressHostname> mapping = CrossVpcIpMappingHandshaker.instance.getCrossVpcIpHostnameMapping();
        assertThat(mapping).isEmpty();
        DatabaseDescriptor.setCrossVpcInternodeCommunication(true);
        CrossVpcIpMappingHandshaker.instance.updateCrossVpcMappings(name, internal, external);
        assertThat(mapping).hasSize(2);
        assertThat(mapping).containsEntry(external, name);
    }

    @Test
    public void updateCrossVpcMappings_noopsWhenCrossVpcDisabled()
    {
        InetAddressHostname name = new InetAddressHostname("host");
        InetAddressIp internal = new InetAddressIp("10.0.0.1");
        InetAddressIp external = new InetAddressIp("20.0.0.1");
        Map<InetAddressIp, InetAddressHostname> mapping = CrossVpcIpMappingHandshaker.instance.getCrossVpcIpHostnameMapping();
        assertThat(mapping).isEmpty();
        DatabaseDescriptor.setCrossVpcInternodeCommunication(false);
        CrossVpcIpMappingHandshaker.instance.updateCrossVpcMappings(name, internal, external);
        assertThat(mapping).isEmpty();
    }

    @Test
    public void start_isEnabledTrueWhenConfigEnabled()
    {
        DatabaseDescriptor.setCrossVpcInternodeCommunication(true);
        CrossVpcIpMappingHandshaker.instance.start();
        assertThat(CrossVpcIpMappingHandshaker.instance.isEnabled()).isTrue();
    }

    @Test
    public void start_isEnabledFalseWhenConfigDisabled()
    {
        DatabaseDescriptor.setCrossVpcIpSwapping(true);
        DatabaseDescriptor.setCrossVpcHostnameSwapping(true);
        DatabaseDescriptor.setCrossVpcInternodeCommunication(false);
        CrossVpcIpMappingHandshaker.instance.start();
        assertThat(CrossVpcIpMappingHandshaker.instance.isEnabled()).isFalse();
    }

    @Test
    public void isEnabled_falseWhenStopped()
    {
        CrossVpcIpMappingHandshaker.instance.start();
        CrossVpcIpMappingHandshaker.instance.stop();
        assertThat(CrossVpcIpMappingHandshaker.instance.isEnabled()).isFalse();
    }

    @Test
    public void isEnabled_falseWhenNotStarted()
    {
        assertThat(CrossVpcIpMappingHandshaker.instance.isEnabled()).isFalse();
    }

    @Test
    public void triggerHandshakeFromSelf_sendsSyn() throws UnknownHostException
    {
        InetAddress target = InetAddress.getByName("localhost");
        DatabaseDescriptor.setCrossVpcIpSwapping(true);
        CrossVpcIpMappingHandshaker.instance.triggerHandshake(new InetAddressHostname("source"),
                                                              new InetAddressIp("10.0.0.1"),
                                                              target);
        Map<String, Long> completed = MessagingService.instance().getSmallMessageCompletedTasks();
        assertThat(completed).containsKey(target.getHostAddress());
        assertThat(completed.get(target.getHostAddress())).isGreaterThanOrEqualTo(0L);
    }

    @Test
    public void triggerHandshakeFromSeeds_onlyActsOnOneRequestPerInterval() throws InterruptedException
    {
        DatabaseDescriptor.setCrossVpcInternodeCommunication(true);
        long first = System.currentTimeMillis();
        CrossVpcIpMappingHandshaker.instance.triggerHandshakeWithSeeds();
        Thread.sleep(1);
        long second = System.currentTimeMillis();
        CrossVpcIpMappingHandshaker.instance.triggerHandshakeWithSeeds();
        assertThat(CrossVpcIpMappingHandshaker.instance.getLastTriggeredHandshakeMillis()).isGreaterThan(first);
        assertThat(CrossVpcIpMappingHandshaker.instance.getLastTriggeredHandshakeMillis()).isLessThan(second);
    }

    private void mockMapping(String hostname,
                             String internalIp,
                             String externalIp,
                             boolean crossVpc,
                             boolean ipSwap,
                             boolean hostSwap) throws UnknownHostException
    {
        InetAddressHostname name = new InetAddressHostname(hostname);
        InetAddressIp internal = new InetAddressIp(internalIp);
        InetAddressIp external = new InetAddressIp(externalIp);
        DatabaseDescriptor.setCrossVpcInternodeCommunication(crossVpc);
        CrossVpcIpMappingHandshaker.instance.updateCrossVpcMappings(name, internal, external);
        // Hostname mapping takes precedence
        DatabaseDescriptor.setCrossVpcHostnameSwapping(hostSwap);
        DatabaseDescriptor.setCrossVpcIpSwapping(ipSwap);
    }
}
