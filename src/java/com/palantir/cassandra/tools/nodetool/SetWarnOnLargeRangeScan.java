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

package com.palantir.cassandra.tools.nodetool;

import io.airlift.command.Arguments;
import io.airlift.command.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(name = "setwarnonlargerangescan", description = "Set whether to warn when a range scan breaches the token range count threshold")
public class SetWarnOnLargeRangeScan extends NodeTool.NodeToolCmd
{
    @Arguments(
    title = "warn_on_large_range_scan",
    usage = "<value>",
    description = "Whether to warn when a range scan breaches the large range scan token range threshold",
    required = true)
    private Boolean doWarn = null;

    @Override
    protected void execute(NodeProbe probe) {
        probe.setWarnOnLargeRangeScan(doWarn);
    }
}
