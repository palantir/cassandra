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

import io.airlift.command.Command;
import io.airlift.command.Option;

import java.util.Set;

import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.net.MessagingServiceMBean;
import org.apache.cassandra.streaming.ProgressInfo;
import org.apache.cassandra.streaming.SessionInfo;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

@Command(name = "netstats", description = "Print network information on provided host (connecting node by default)")
public class NetStats extends NodeToolCmd
{
    @Option(title = "human_readable",
            name = {"-H", "--human-readable"},
            description = "Display bytes in human readable form, i.e. KB, MB, GB, TB")
    private boolean humanReadable = false;

    @Override
    public void execute(NodeProbe probe)
    {
        probe.getOutput().printf("Mode: %s%n", probe.getOperationMode());
        Set<StreamState> statuses = probe.getStreamStatus();
        if (statuses.isEmpty())
            probe.getOutput().println("Not sending any streams.");
        for (StreamState status : statuses)
        {
            probe.getOutput().printf("%s %s%n", status.description, status.planId.toString());
            for (SessionInfo info : status.sessions)
            {
                probe.getOutput().printf("    %s", info.peer.toString());
                // print private IP when it is used
                if (!info.peer.equals(info.connecting))
                {
                    probe.getOutput().printf(" (using %s)", info.connecting.toString());
                }
                probe.getOutput().printf("%n");
                if (!info.receivingSummaries.isEmpty())
                {
                    if (humanReadable)
                        probe.getOutput().printf("        Receiving %d files, %s total. Already received %d files, %s total%n", info.getTotalFilesToReceive(), FileUtils.stringifyFileSize(info.getTotalSizeToReceive()), info.getTotalFilesReceived(), FileUtils.stringifyFileSize(info.getTotalSizeReceived()));
                    else
                        probe.getOutput().printf("        Receiving %d files, %d bytes total. Already received %d files, %d bytes total%n", info.getTotalFilesToReceive(), info.getTotalSizeToReceive(), info.getTotalFilesReceived(), info.getTotalSizeReceived());
                    for (ProgressInfo progress : info.getReceivingFiles())
                    {
                        probe.getOutput().printf("            %s%n", progress.toString());
                    }
                }
                if (!info.sendingSummaries.isEmpty())
                {
                    if (humanReadable)
                        probe.getOutput().printf("        Sending %d files, %s total. Already sent %d files, %s total%n", info.getTotalFilesToSend(), FileUtils.stringifyFileSize(info.getTotalSizeToSend()), info.getTotalFilesSent(), FileUtils.stringifyFileSize(info.getTotalSizeSent()));
                    else
                        probe.getOutput().printf("        Sending %d files, %d bytes total. Already sent %d files, %d bytes total%n", info.getTotalFilesToSend(), info.getTotalSizeToSend(), info.getTotalFilesSent(), info.getTotalSizeSent());
                    for (ProgressInfo progress : info.getSendingFiles())
                    {
                        probe.getOutput().printf("            %s%n", progress.toString());
                    }
                }
            }
        }

        if (!probe.isStarting())
        {
            probe.getOutput().printf("Read Repair Statistics:%nAttempted: %d%nMismatch (Blocking): %d%nMismatch (Background): %d%n", probe.getReadRepairAttempted(), probe.getReadRepairRepairedBlocking(), probe.getReadRepairRepairedBackground());

            MessagingServiceMBean ms = probe.getMessagingServiceProxy();
            probe.getOutput().printf("%-25s", "Pool Name");
            probe.getOutput().printf("%10s", "Active");
            probe.getOutput().printf("%10s", "Pending");
            probe.getOutput().printf("%15s", "Completed");
            probe.getOutput().printf("%10s%n", "Dropped");

            int pending;
            long completed;
            long dropped;

            pending = 0;
            for (int n : ms.getLargeMessagePendingTasks().values())
                pending += n;
            completed = 0;
            for (long n : ms.getLargeMessageCompletedTasks().values())
                completed += n;
            dropped = 0;
            for (long n : ms.getLargeMessageDroppedTasks().values())
                dropped += n;
            probe.getOutput().printf("%-25s%10s%10s%15s%10s%n", "Large messages", "n/a", pending, completed, dropped);

            pending = 0;
            for (int n : ms.getSmallMessagePendingTasks().values())
                pending += n;
            completed = 0;
            for (long n : ms.getSmallMessageCompletedTasks().values())
                completed += n;
            dropped = 0;
            for (long n : ms.getSmallMessageDroppedTasks().values())
                dropped += n;
            probe.getOutput().printf("%-25s%10s%10s%15s%10s%n", "Small messages", "n/a", pending, completed, dropped);

            pending = 0;
            for (int n : ms.getGossipMessagePendingTasks().values())
                pending += n;
            completed = 0;
            for (long n : ms.getGossipMessageCompletedTasks().values())
                completed += n;
            dropped = 0;
            for (long n : ms.getGossipMessageDroppedTasks().values())
                dropped += n;
            probe.getOutput().printf("%-25s%10s%10s%15s%10s%n", "Gossip messages", "n/a", pending, completed, dropped);
        }
    }
}
