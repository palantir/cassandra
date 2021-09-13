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
import java.util.Set;
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
    public final static Duration interval = Duration.ofSeconds(1);

    private CrossVpcIpMappingHandshaker() {}

    public static void triggerHandshakeFromSelf(Set<InetAddress> targets)
    {
        InetAddressHostname selfName = new InetAddressHostname(FBUtilities.getLocalAddress().getHostName());
        InetAddressIp selfIp = new InetAddressIp(FBUtilities.getBroadcastAddress().getHostAddress());
        logger.trace("Triggering handshakes from {}/{} to {}", selfName, selfIp, targets);
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
    static void triggerHandshake(InetAddressHostname sourceName, InetAddressIp sourceIp, InetAddress target)
    {
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
        if (!DatabaseDescriptor.isCrossVpcIpSwappingEnabled()) {
            logger.info("Cross VPC IP Swapping is disabled. Not scheduling handshake task.");
            return;
        }
        if (isEnabled())
        {
            logger.info("Cross VPC IP Swapping already enabled and scheduled. Ignoring extra start() call.");
            return;
        }
        logger.info("Started running CrossVpcIpMappingTask at interval of {} ms",
                     CrossVpcIpMappingHandshaker.interval);
        scheduledCVIMTask = executor.scheduleWithFixedDelay(() -> {
            try
            {
                // seeds should be provided via config by hostname with our setup. Could probably get fancier with deciding
                // who we currently are going to handshake with like the Gossiper does
                Set<InetAddress> seeds = DatabaseDescriptor.getSeeds()
                                                           .stream()
                                                           .filter(seed -> !seed.equals(FBUtilities.getBroadcastAddress()))
                                                           .collect(Collectors.toSet());

                CrossVpcIpMappingHandshaker.triggerHandshakeFromSelf(seeds);
            } catch (Exception e)
            {
                logger.error("Caught exception trying to run scheduled CrossVpcIpMapping handshake task", e);
            }
        }, 0L, CrossVpcIpMappingHandshaker.interval.toMillis(), TimeUnit.MILLISECONDS);
    }

    public boolean isEnabled()
    {
        return (scheduledCVIMTask != null) && (!scheduledCVIMTask.isCancelled());
    }

    public void stop()
    {
        if (scheduledCVIMTask != null)
        {
            scheduledCVIMTask.cancel(false);
            logger.trace("Stopped running CrossVpcIpMappingTask at interval");
        }
    }
}
