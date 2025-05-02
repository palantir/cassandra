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

import java.util.HashSet;
import java.util.List;

import io.airlift.command.Arguments;
import io.airlift.command.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(name = "setcoercereadconsistencyallkeyspaces", description = "Upgrade the consistency level used by all QUORUM requests to ALL, regardless of what is set by the client, for the provided keyspaces")
public class SetCoerceReadConsistencyAllKeyspaces extends NodeTool.NodeToolCmd
{
    @Arguments(title = "keyspaces", usage = "<keyspace> <keyspace> <keyspace>", description = "A space-delimited list of keyspaces to set coercion for", required = true)
    private List<String> keyspaces;

    @Override
    public void execute(NodeProbe probe)
    {
        probe.setCoerceReadConsistencyAllKeyspaces(new HashSet<>(keyspaces));
    }
}
