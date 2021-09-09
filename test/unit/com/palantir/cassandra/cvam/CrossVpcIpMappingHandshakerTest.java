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

package com.palantir.cassandra.cvam;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.MessagingService;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class CrossVpcIpMappingHandshakerTest
{
    @Before
    public void before()
    {
        CrossVpcIpMappingHandshaker.instance.stop();
    }

    @Test
    @Ignore
    public void start_startsPpamTask() throws InterruptedException
    {
        CrossVpcIpMappingHandshaker.instance.stop();
        long initial = CrossVpcIpMappingHandshaker.instance.numTasks.get();
        CrossVpcIpMappingHandshaker.instance.start();
        Thread.sleep(1000);
        assertThat(CrossVpcIpMappingHandshaker.instance.numTasks.get()).isGreaterThan(initial);
    }

    @Test
    public void stop_stopsPpamTask()
    {}

    @Test
    public void isEnabled_trueWhenStarted()
    {
        CrossVpcIpMappingHandshaker.instance.start();
        assertThat(CrossVpcIpMappingHandshaker.instance.isEnabled()).isTrue();
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
    @Ignore
    public void ppamTask_sendsSyn() throws UnknownHostException
    {
        InetAddress target = InetAddress.getByName("localhost");
        Set<InetAddress> targets = new HashSet<>();
        targets.add(target);
        CrossVpcIpMappingHandshaker.triggerHandshakeFromSelf(targets);
        Map<String, Long> completed = MessagingService.instance().getSmallMessageCompletedTasks();
        assertThat(completed).containsKey(target.getHostAddress());
        assertThat(completed.get(target.getHostAddress())).isGreaterThanOrEqualTo(0L);
    }

    @Test
    public void triggerHandshakeFromSelf_sendsSyn() throws UnknownHostException
    {
        InetAddress target = InetAddress.getByName("localhost");
        CrossVpcIpMappingHandshaker.triggerHandshake(new InetAddressHostname("source"), new InetAddressIp("10.0.0.1"), target);
        Map<String, Long> completed = MessagingService.instance().getSmallMessageCompletedTasks();
        assertThat(completed).containsKey(target.getHostAddress());
        assertThat(completed.get(target.getHostAddress())).isGreaterThanOrEqualTo(0L);
    }

    @Test
    public void maybeSwapPrivateForPublicAddress_noopWhenDisabled() throws UnknownHostException
    {
        InetAddressHostname sourceName = new InetAddressHostname("localhost");
        InetAddressIp sourceInternal = new InetAddressIp("127.0.0.1");
        InetAddressIp sourceExternal = new InetAddressIp("2.0.0.0");
        CrossVpcIpMappingHandshaker.instance.updateCrossVpcIpMapping(sourceName, sourceInternal, sourceExternal);

        DatabaseDescriptor.setCrossVpcIpSwapping(false);
        assertThat(DatabaseDescriptor.crossVpcIpSwappingEnabled()).isFalse();
        InetAddress initial = InetAddress.getByName("localhost");
        assertThat(CrossVpcIpMappingHandshaker.instance.maybeSwapPrivateForPublicAddress(initial)).isEqualTo(initial);
    }

    @Test
    public void maybeSwapPrivateForPublicAddress_swapsWhenMappingAvailable() throws UnknownHostException
    {
        InetAddressHostname sourceName = new InetAddressHostname("localhost");
        InetAddressIp sourceInternal = new InetAddressIp("127.0.0.1");
        InetAddressIp sourceExternal = new InetAddressIp("2.0.0.0");
        CrossVpcIpMappingHandshaker.instance.updateCrossVpcIpMapping(sourceName, sourceInternal, sourceExternal);

        DatabaseDescriptor.setCrossVpcIpSwapping(true);
        assertThat(DatabaseDescriptor.crossVpcIpSwappingEnabled()).isTrue();
        assertThat(CrossVpcIpMappingHandshaker.instance.maybeSwapPrivateForPublicAddress(InetAddress.getByName("localhost"))).isEqualTo(InetAddress.getByName(sourceExternal.toString()));
    }

    @Test
    public void maybeSwapPrivateForPublicAddress_noopsWhenNoMappingAvailable() throws UnknownHostException
    {
        InetAddress source = InetAddress.getByName("localhost");
        CrossVpcIpMappingHandshaker.instance.clearCrossVpcIpMapping();
        DatabaseDescriptor.setCrossVpcIpSwapping(true);
        assertThat(DatabaseDescriptor.crossVpcIpSwappingEnabled()).isTrue();
        assertThat(CrossVpcIpMappingHandshaker.instance.maybeSwapPrivateForPublicAddress(source)).isEqualTo(source);
    }
}
