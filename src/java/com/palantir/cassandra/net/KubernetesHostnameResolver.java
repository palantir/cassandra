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
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

public class KubernetesHostnameResolver implements HostnameResolver
{
    private static final Pattern podHostnamePattern = Pattern.compile("^(.*)-(.*)\\.(.*)\\.(.*)\\.svc\\.cluster\\.local$");

    private final Supplier<Integer> endpointCountSupplier;
    private final DnsClient dnsClient;

    public KubernetesHostnameResolver(Supplier<Integer> endpointCountSupplier)
    {
        this.endpointCountSupplier = endpointCountSupplier;
        this.dnsClient = new DefaultDnsClient();
    }

    @VisibleForTesting
    KubernetesHostnameResolver(Supplier<Integer> endpointCountSupplier, DnsClient dnsClient)
    {
        this.endpointCountSupplier = endpointCountSupplier;
        this.dnsClient = dnsClient;
    }

    public String getHostname(InetAddress address)
    {
        String localHostname = dnsClient.getLocalHostname();
        Matcher matcher = podHostnamePattern.matcher(localHostname);
        Preconditions.checkState(matcher.matches(), "Local hostname does not match expected pattern: %s", localHostname);

        String stsName = matcher.group(1);
        String namespace = matcher.group(4);

        int endpointCount = endpointCountSupplier.get();
        for (int i = 0; i < endpointCount; i++)
        {
            String hostname = getKubernetesPodUri(stsName, namespace, i);
            if (address.equals(dnsClient.maybeGetByName(hostname)))
            {
                return hostname;
            }
        }

        throw new RuntimeException("Could not resolve hostname for address: " + address);
    }


    private static String getKubernetesPodUri(String stsName, String namespace, int podIdx) {
        return String.format("%s-%s.%s.%s.svc.cluster.local", stsName, podIdx, stsName, namespace);
    }
}
