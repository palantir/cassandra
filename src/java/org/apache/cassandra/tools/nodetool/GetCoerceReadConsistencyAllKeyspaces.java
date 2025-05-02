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

import java.util.Set;

import io.airlift.command.Command;
import io.airlift.command.Option;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(name = "getcoercereadconsistencyallkeyspaces", description = "Get list of keyspaces for which read consistency level is coerced to ALL, or if, a keyspace is provided, whether that keyspace is coerced or not")
public class GetCoerceReadConsistencyAllKeyspaces extends NodeTool.NodeToolCmd
{

    @Option(name = {"-k", "--keyspace"}, description = "The keyspace to check for read consistency level coercion")
    private String keyspace;

    @Override
    public void execute(NodeProbe probe)
    {
        if (keyspace == null || keyspace.isEmpty()) {
            Set<String> coercedKeyspaces = probe.getCoerceReadConsistencyAllKeyspaces();
            if (coercedKeyspaces.isEmpty()) {
                probe.output().out.println("All keyspaces coerced QUORUM read consistency to ALL");
            } else {
                probe.output().out.println(probe.getCoerceReadConsistencyAllKeyspaces());
            }
        } else {
            probe.output().out.println(probe.getCoerceReadConsistencyAllForKeyspace(keyspace));
        }
    }
}
