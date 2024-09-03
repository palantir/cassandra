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

package com.palantir.cassandra.net;

import java.net.InetAddress;

import com.google.common.collect.ImmutableMap;
import com.google.common.net.InetAddresses;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KubernetesHostnameResolverTest
{

    private DnsClient dnsClient;

    @Before
    public void before()
    {
        dnsClient = mock(DnsClient.class);

        when(dnsClient.getLocalHostname()).thenReturn(hostname(0));
        when(dnsClient.maybeGetByName(hostname(0))).thenReturn(InetAddresses.forString("10.0.0.1"));
        when(dnsClient.maybeGetByName(hostname(1))).thenReturn(InetAddresses.forString("10.0.0.2"));
        when(dnsClient.maybeGetByName(hostname(2))).thenReturn(InetAddresses.forString("10.0.0.3"));
    }

    @Test
    public void testResolve()
    {
        KubernetesHostnameResolver resolver = new KubernetesHostnameResolver(() -> 3, dnsClient);

        String actual = resolver.getHostname(InetAddresses.forString("10.0.0.3"));
        assertThat(actual).isEqualTo(hostname(2));
    }

    @Test
    public void testAddressNotResolvable()
    {
        KubernetesHostnameResolver resolver = new KubernetesHostnameResolver(() -> 3, dnsClient);

        assertThatThrownBy(() -> resolver.getHostname(InetAddresses.forString("10.0.0.4"))).isInstanceOf(RuntimeException.class);
    }

    @Test
    public void testAddressNotResolvableWhenEndpointCountWrong()
    {
        KubernetesHostnameResolver resolver = new KubernetesHostnameResolver(() -> 2, dnsClient);

        assertThatThrownBy(() -> resolver.getHostname(InetAddresses.forString("10.0.0.3"))).isInstanceOf(RuntimeException.class);
    }

    @Test
    public void testResolvesWhenOtherAddressIsUnresolvable()
    {
        when(dnsClient.maybeGetByName(hostname(1))).thenReturn(null);

        KubernetesHostnameResolver resolver = new KubernetesHostnameResolver(() -> 2, dnsClient);

        assertThatThrownBy(() -> resolver.getHostname(InetAddresses.forString("10.0.0.3"))).isInstanceOf(RuntimeException.class);
    }

    private static String hostname(int podIdx) {
        return String.format("stateful-set-name-%d.stateful-set-name.data-namespace.svc.cluster.local", podIdx);
    }
}
