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
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
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
 * {@link org.apache.cassandra.gms.GossipDigestAck} messages to seed nodes and managing the internal/external node IP
 * mapping for cross-vpc connections.
 */
public class CrossVpcIpMappingHandshaker
{
    private static final Logger logger = LoggerFactory.getLogger(CrossVpcIpMappingHandshaker.class);
    public static final CrossVpcIpMappingHandshaker instance = new CrossVpcIpMappingHandshaker();
    // A single cross-VPC hostname can resolve to 3 valid IPs. More on the key/values
    // here in the Edge Cases + Questions section of the RFC
    private final ConcurrentHashMap<InetAddressIp, InetAddressIp> crossVpcIpMappings;
    private static final DebuggableScheduledThreadPoolExecutor executor =
                                    new DebuggableScheduledThreadPoolExecutor("CrossVpcIpMappingTasks");
    private volatile ScheduledFuture<?> scheduledPPAMTask;
    public final static int intervalInMillis = 30000;

    @VisibleForTesting
    final AtomicLong numTasks = new AtomicLong();

    private CrossVpcIpMappingHandshaker()
    {
        this.crossVpcIpMappings = new ConcurrentHashMap<>();
    }

    @VisibleForTesting
    ConcurrentHashMap<InetAddressIp, InetAddressIp> getCrossVpcIpMapping()
    {
        return crossVpcIpMappings;
    }

    @VisibleForTesting
    void clearCrossVpcIpMapping()
    {
        crossVpcIpMappings.clear();
    }

    public void updateCrossVpcIpMapping(InetAddressHostname host, InetAddressIp key, InetAddressIp newValue)
    {

        InetAddressIp old = CrossVpcIpMappingHandshaker.instance.getCrossVpcIpMapping().put(key, newValue);
        if (!Objects.equals(newValue, old))
        {
            logger.trace("Updated private/public IP mapping for {}/{} from {} to {}", host, key, old, newValue);
        }
    }

    public InetAddress maybeSwapPrivateForPublicAddress(InetAddress endpoint) throws UnknownHostException
    {
        Map<InetAddressIp, InetAddressIp> endpointMappings = instance.getCrossVpcIpMapping();
        InetAddressIp proposedAddress = new InetAddressIp(endpoint.getHostAddress());
        if (endpointMappings.containsKey(proposedAddress) && DatabaseDescriptor.crossVpcIpSwappingEnabled())
        {
            InetAddressIp publicAddress = endpointMappings.get(proposedAddress);
            logger.trace("Swapped address {} for {}", endpoint, publicAddress);
            return InetAddress.getByName(publicAddress.toString());
        }
        return endpoint;
    }

    // This could potentially be invoked for unreachable nodes by the Gossiper
    // Although, not sure if we would really use this as the Gossiper might not include
    // the target's hostname in which case we would throw
    public static void triggerHandshakeFromSelf(Set<InetAddress> targets)
    {
        instance.numTasks.getAndIncrement();
        InetAddressHostname selfName = new InetAddressHostname(FBUtilities.getLocalAddress().getHostName());
        InetAddressIp selfIp = new InetAddressIp(FBUtilities.getBroadcastAddress().getHostAddress());
        logger.trace("Triggering handshakes from {}/{} to {}", selfName, selfIp, targets);
        targets.forEach(target -> triggerHandshake(selfName, selfIp, target));
    }

    @VisibleForTesting
    static void triggerHandshake(InetAddressHostname sourceName, InetAddressIp sourceIp, InetAddress target)
    {
        CrossVpcIpMappingSyn syn = new CrossVpcIpMappingSyn(
            sourceName,
            sourceIp,
            new InetAddressHostname(target.getHostName()),
            new InetAddressIp(target.getHostAddress())
        );

        MessageOut<CrossVpcIpMappingSyn> synMessage = new MessageOut<>(
        MessagingService.Verb.CROSS_VPC_IP_MAPPING_SYN,
        syn,
        CrossVpcIpMappingSyn.serializer);
        MessagingService.instance().sendOneWay(synMessage, target);
    }

    // This could be the task that's run on a schedule
    private static class CrossVpcIpMappingTask implements Runnable
    {
        public void run()
        {
            // seeds should be provided via config by hostname with our setup. Could probably get fancier with deciding
            // who we currently are going to handshake with like the Gossiper does
            Set<InetAddress> seeds = DatabaseDescriptor.getSeeds().stream()
                                                       .filter(seed -> !seed.equals(FBUtilities.getBroadcastAddress()))
                                                       .collect(Collectors.toSet());

            CrossVpcIpMappingHandshaker.triggerHandshakeFromSelf(seeds);
        }
    }

    public void start()
    {
        logger.trace("Started running CrossVpcIpMappingTask at interval of {} ms",
                     CrossVpcIpMappingHandshaker.intervalInMillis);
        scheduledPPAMTask = executor.scheduleWithFixedDelay(new CrossVpcIpMappingTask(),
                                                            0L,
                                                            CrossVpcIpMappingHandshaker.intervalInMillis,
                                                            TimeUnit.MILLISECONDS);
    }

    public boolean isEnabled()
    {
        return (scheduledPPAMTask != null) && (!scheduledPPAMTask.isCancelled());
    }

    public void stop()
    {
        if (scheduledPPAMTask != null)
        {
            scheduledPPAMTask.cancel(false);
            logger.trace("Stopped running CrossVpcIpMappingTask at interval");
        }
    }
}
