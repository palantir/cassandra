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
import java.time.Duration;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.DebuggableScheduledThreadPoolExecutor;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Similar to {@link org.apache.cassandra.gms.Gossiper}, this class is responsible for sending
 * {@link CrossVpcIpMappingSyn} messages to seed nodes and managing the internal/external node IP
 * mapping for cross-vpc connections.
 */
public class CrossVpcIpMappingHandshaker
{
    private static final Logger logger = LoggerFactory.getLogger(CrossVpcIpMappingHandshaker.class);
    public static final CrossVpcIpMappingHandshaker instance = new CrossVpcIpMappingHandshaker();
    private static final DebuggableScheduledThreadPoolExecutor executor = new DebuggableScheduledThreadPoolExecutor(
                                                                                "CrossVpcIpMappingTasks");
    private volatile ScheduledFuture<?> scheduledCVIMTask;
    public final static Duration scheduledInterval = Duration.ofSeconds(1);
    public final static Duration minHandshakeInterval = Duration.ofMillis(25);
    private static volatile long lastTriggeredHandshakeMillis;
    private final ConcurrentHashMap<InetAddressIp, InetAddressIp> privatePublicIpMappings;
    private final ConcurrentHashMap<InetAddressIp, InetAddressHostname> privateIpHostnameMappings;

    private CrossVpcIpMappingHandshaker() {
        this.privatePublicIpMappings = new ConcurrentHashMap<>();
        this.privateIpHostnameMappings = new ConcurrentHashMap<>();
    }

    public void updateCrossVpcMappings(InetAddressHostname host, InetAddressIp internalIp, InetAddressIp externalIp)
    {
        InetAddressIp oldExternalIp = this.privatePublicIpMappings.get(internalIp);
        if (!externalIp.equals(oldExternalIp))
        {
            this.privatePublicIpMappings.put(internalIp, externalIp);
            logger.trace("Updated private/public IP mapping for {} from {}->{} to {}", host, internalIp, oldExternalIp, externalIp);
        }

        InetAddressHostname old = this.privateIpHostnameMappings.get(internalIp);
        if (!host.equals(old))
        {
            this.privateIpHostnameMappings.put(internalIp, host);
            logger.trace("Updated private IP/hostname mapping from {}->{} to {}", internalIp, old, host);
        }
    }

    public InetAddress maybeSwapAddress(InetAddress endpoint)
    {
        InetAddressIp proposedAddress = new InetAddressIp(endpoint.getHostAddress());
        if (privateIpHostnameMappings.containsKey(proposedAddress) && DatabaseDescriptor.isCrossVpcHostnameSwappingEnabled())
        {
            return maybeSwapHostname(endpoint);
        }

        if (privatePublicIpMappings.containsKey(proposedAddress) && DatabaseDescriptor.isCrossVpcIpSwappingEnabled())
        {
            return maybeSwapIp(endpoint);
        }
        return endpoint;
    }

    private InetAddress maybeSwapHostname(InetAddress endpoint)
    {
        InetAddressHostname hostname = privateIpHostnameMappings.get(new InetAddressIp(endpoint.getHostAddress()));
        logger.trace("Performing DNS lookup for host {}", hostname);
        InetAddress resolved;
        try {
            resolved = InetAddress.getByName(hostname.toString());
        } catch (UnknownHostException e)
        {
            logger.error("Cross VPC mapping contains unresolvable hostname for endpoint {} (unresolved: {})", endpoint, hostname);
            return endpoint;
        }
        if (!resolved.equals(endpoint))
        {
            logger.trace("DNS-resolved address different than provided endpoint. Swapping. provided: {} resolved: {}", endpoint, resolved);
            return resolved;
        }
        return endpoint;
    }

    private InetAddress maybeSwapIp(InetAddress endpoint)
    {
        InetAddressIp proposedAddress = new InetAddressIp(endpoint.getHostAddress());
        InetAddressIp result = privatePublicIpMappings.get(proposedAddress);
        if (!result.equals(proposedAddress))
        {
            logger.trace("Swapped address {} for {}", endpoint, result);
            try
            {
                return InetAddress.getByName(result.toString());
            }
            catch (UnknownHostException e)
            {
                logger.error("Failed to resolve host for externally-mapped IP {}->{}. Ensure the address mapping does not contain hostnames", endpoint, result);
            }
        }
        return endpoint;
    }

    public void triggerHandshakeWithSeeds()
    {
        try
        {
            if (System.currentTimeMillis() - lastTriggeredHandshakeMillis < minHandshakeInterval.toMillis())
            {
                logger.trace("Ignoring handshake request as last handshake is too recent");
                return;
            }
            // seeds should be provided via config by hostname with our setup. Could probably get fancier with deciding
            // who we currently are going to handshake with like the Gossiper does
            Set<InetAddress> seeds = DatabaseDescriptor.getSeeds()
                                                       .stream()
                                                       .filter(seed -> !seed.equals(FBUtilities.getBroadcastAddress()))
                                                       .collect(Collectors.toSet());

            triggerHandshakeFromSelf(seeds);
        }
        catch (Exception e)
        {
            logger.error("Caught exception trying to trigger CrossVpcIpMapping handshake with seeds", e);
        }
    }

    @VisibleForTesting
    synchronized void triggerHandshakeFromSelf(Set<InetAddress> targets)
    {
        lastTriggeredHandshakeMillis = System.currentTimeMillis();
        InetAddressHostname selfName = new InetAddressHostname(FBUtilities.getLocalAddress().getHostName());
        InetAddressIp selfIp = new InetAddressIp(FBUtilities.getBroadcastAddress().getHostAddress());
        targets.forEach(target -> {
            try
            {
                triggerHandshake(selfName, selfIp, target);
            } catch (Exception e)
            {
                logger.error("Caught exception trying to trigger handshake from {}/{} to {}", selfName, selfIp, target);
            }
        });
    }

    @VisibleForTesting
    void triggerHandshake(InetAddressHostname sourceName, InetAddressIp sourceIp, InetAddress target)
    {
        if (!DatabaseDescriptor.isCrossVpcIpSwappingEnabled())
        {
            return;
        }
        logger.trace("Triggering cross VPC IP swapping handshake from {}/{} to {}", sourceName, sourceIp, target);
        CrossVpcIpMappingSyn syn = new CrossVpcIpMappingSyn(sourceName,
                                                            sourceIp,
                                                            new InetAddressHostname(target.getHostName()),
                                                            new InetAddressIp(target.getHostAddress()));

        MessageOut<CrossVpcIpMappingSyn> synMessage = new MessageOut<>(MessagingService.Verb.CROSS_VPC_IP_MAPPING_SYN,
                                                                       syn,
                                                                       CrossVpcIpMappingSyn.serializer);
        MessagingService.instance().sendOneWay(synMessage, target);
    }

    public void start()
    {
        if (!DatabaseDescriptor.isCrossVpcIpSwappingEnabled() && DatabaseDescriptor.isCrossVpcHostnameSwappingEnabled()) {
            logger.warn("Cross VPC IP Swapping is disabled. Not scheduling handshake task. Set " +
                        "cross_vpc_ip_swapping_enabled=true or cross_vpc_hostname_swapping_enabled=true if desired");
            return;
        }
        if (isEnabled())
        {
            logger.info("Cross VPC IP Swapping already enabled and scheduled. Ignoring extra start() call.");
            return;
        }
        logger.info("Started running CrossVpcIpMappingTask at interval of {}",
                     CrossVpcIpMappingHandshaker.scheduledInterval);
        scheduledCVIMTask = executor.scheduleWithFixedDelay(() -> {
            try
            {
                triggerHandshakeWithSeeds();
            } catch (Exception e)
            {
                logger.error("Caught exception trying to run scheduled CrossVpcIpMapping handshake task", e);
            }
        }, 0L, CrossVpcIpMappingHandshaker.scheduledInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    public boolean isEnabled()
    {
        return (scheduledCVIMTask != null) && (!scheduledCVIMTask.isCancelled());
    }

    public void stop()
    {
        if (scheduledCVIMTask != null)
        {
            logger.warn("Stopping CrossVpcIpMappingTask at interval after operator request");
            scheduledCVIMTask.cancel(false);
        }
    }

    @VisibleForTesting
    Map<InetAddressIp, InetAddressIp> getCrossVpcIpMapping()
    {
        return this.privatePublicIpMappings;
    }

    @VisibleForTesting
    Map<InetAddressIp, InetAddressHostname> getCrossVpcIpHostnameMapping()
    {
        return this.privateIpHostnameMappings;
    }

    @VisibleForTesting
    void clearMappings()
    {
        this.privateIpHostnameMappings.clear();
        this.privatePublicIpMappings.clear();
    }
}
