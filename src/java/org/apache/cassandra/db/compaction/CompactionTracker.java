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

package org.apache.cassandra.db.compaction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

public class CompactionTracker
{

    // a synchronized identity set of running tasks to their compaction info
    private final Set<CompactionInfo.Holder> compactions = Collections.synchronizedSet(Collections.newSetFromMap(new IdentityHashMap<CompactionInfo.Holder, Boolean>()));

    public void beginCompaction(CompactionInfo.Holder ci)
    {
        // notify
        ci.started();
        compactions.add(ci);
    }

    public void finishCompaction(CompactionInfo.Holder ci)
    {
        // notify
        ci.finished();
        compactions.remove(ci);
    }

    public List<CompactionInfo.Holder> getCompactions()
    {
        return new ArrayList<CompactionInfo.Holder>(compactions);
    }

    public long getTotalCompletedBytes()
    {
        return getCompactions().stream()
                               .map(holder -> holder.getCompactionInfo().getCompleted())
                               .mapToLong(Long::longValue)
                               .sum();
    }
}
