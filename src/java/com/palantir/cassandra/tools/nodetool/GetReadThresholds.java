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

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

@Command(name = "getreadthresholds", description = "Print the various warn and failure thresholds for reads")
public class GetReadThresholds extends NodeToolCmd {
    @Override
    public void execute(NodeProbe probe)
    {
        System.out.println("Row count warn threshold: " + probe.getRowCountWarnThreshold());
        System.out.println("Row count failure threshold: " + probe.getRowCountFailureThreshold());
        System.out.println("Tombstone warn threshold: " + probe.getTombstoneWarnThreshold());
        System.out.println("Tombstone failure threshold: " + probe.getTombstoneFailureThreshold());
    }
}
