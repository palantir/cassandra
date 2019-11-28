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
package org.apache.cassandra.tools.nodetool;

import static java.lang.String.format;
import io.airlift.command.Command;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;

import javax.management.InstanceNotFoundException;

import org.apache.cassandra.locator.DynamicEndpointSnitch;
import org.apache.cassandra.locator.DynamicEndpointSnitchMBean;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

@Command(name = "dynamicendpointsnitchstats", description = "Print the dynamic snitch configurations of a cluster and the current scores of all nodes")
public class DynamicEndpointSnitchStats extends NodeToolCmd
{
    @Override
    public void execute(NodeProbe probe)
    {
        try
        {
            DynamicEndpointSnitchMBean dynamicSnitchProxy = probe.getDynamicEndpointSnitchProxy();
            // display snitch configuration
            System.out.println("Dynamic Endpoint Snitch Configuration:");
            System.out.println("\tUpdate Interval (ms): " + dynamicSnitchProxy.getUpdateInterval());
            System.out.println("\tReset Interval (ms): " + dynamicSnitchProxy.getResetInterval());
            System.out.println("\tBadness Threshold: " + dynamicSnitchProxy.getBadnessThreshold());
            System.out.println("\tSubsnitch: " + dynamicSnitchProxy.getSubsnitchClassName());
            System.out.println("\tSeverity: " + dynamicSnitchProxy.getSeverity());
            // display snitch scores for each node
            System.out.println("Dynamic Endpoint Snitch Scores:");
            Map<InetAddress, Double> snitchScores = dynamicSnitchProxy.getScores();
            for (InetAddress address : snitchScores.keySet())
            {
                System.out.println(format("\t\t%s: %s%n", address.getCanonicalHostName(), snitchScores.get(address)));
            }
        } catch (RuntimeException e) {
            if ((e.getCause() instanceof InstanceNotFoundException)) {
                System.out.println("Error getting DynamicEndpointSnitch proxy--Dynamic snitch may not be enabled on this cluster.");
            }
        }

    }
}