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

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

import io.airlift.command.Arguments;
import io.airlift.command.Command;

@Command(name = "setrowcountfailurethreshold", description = "Set the row count failure threshold for reads")
public class SetRowCountFailureThreshold extends NodeToolCmd {
    @Arguments(
            title = "rowcount_failure_treshold",
            usage = "<value>",
            description = "Number of rows in a single read",
            required = true)
    private Integer threshold = null;
    
    @Override
    protected void execute(NodeProbe probe) {
        probe.setRowCountFailureThreshold(threshold);
    }
}
