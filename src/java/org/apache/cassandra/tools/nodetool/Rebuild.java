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

import io.airlift.command.Arguments;
import io.airlift.command.Command;

import io.airlift.command.Option;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

@Command(name = "rebuild", description = "Rebuild data by streaming from other nodes (similarly to bootstrap)")
public class Rebuild extends NodeToolCmd
{
    @Arguments(usage = "<src-dc-name>", description = "Name of DC from which to select sources for streaming. By default, pick any DC")
    private String sourceDataCenterName = null;

    @Option(title = "specific_keyspace",
    name = {"-ks", "--keyspace"},
    description = "Use -ks to rebuild specific keyspace.")
    private String keyspace = null;

    @Override
    public void execute(NodeProbe probe)
    {
        probe.rebuild(sourceDataCenterName, keyspace);
    }
}