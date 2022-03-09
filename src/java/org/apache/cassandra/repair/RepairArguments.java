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

package org.apache.cassandra.repair;

import java.util.Objects;
import javax.annotation.Nullable;

import org.apache.cassandra.repair.messages.RepairOption;

public class RepairArguments
{
    private final String keyspace;
    private final RepairOption options;

    public RepairArguments(String keyspace, RepairOption options)
    {
        this.keyspace = keyspace;
        this.options = options;
    }

    public String keyspace()
    {
        return keyspace;
    }

    public RepairOption repairOptions()
    {
        return options;
    }

    @Override
    public boolean equals(@Nullable Object another) {
        if (this == another) return true;
        return another instanceof RepairArguments
               && equalTo((RepairArguments) another);
    }

    private boolean equalTo(RepairArguments another) {
        return keyspace.equals(another.keyspace)
               && options.equals(another.options);
    }

    @Override
    public int hashCode() {
        int h = 5381;
        h += (h << 5) + options.hashCode();
        h += (h << 5) + keyspace.hashCode();
        return h;
    }
}
