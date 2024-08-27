/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.cassandra.tools.nodetool;

import io.airlift.command.Command;

import java.util.Map;

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

@Command(name = "forceterminaterepairsessions", description = "Terminates active repair sessions on the connected node")
public class ForceTerminateRepairSessions extends NodeToolCmd
{
    @Override
    public void execute(NodeProbe probe)
    {
        probe.output().out.println("Attempting to force terminate all running and already queued repair sessions on this host...");
        probe.forceTerminateActiveRepairSessions();
        probe.output().out.println("Done.");
    }
}
