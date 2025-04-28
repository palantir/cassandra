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

package com.palantir.cassandra.objects;

import javax.annotation.Nullable;

import com.palantir.logsafe.SafeArg;

public final class CompactionEstimationWrapper
{
    private final long estimatedSSTables;
    private final long expectedTotalWriteSize;
    private final long liveSpaceUsedByInProgressCompactions;

    private CompactionEstimationWrapper(long estimatedSSTables, long expectedTotalWriteSize, long liveSpaceUsedByInProgressCompactions)
    {
        this.estimatedSSTables = estimatedSSTables;
        this.expectedTotalWriteSize = expectedTotalWriteSize;
        this.liveSpaceUsedByInProgressCompactions = liveSpaceUsedByInProgressCompactions;
    }

    public long getEstimatedSSTables()
    {
        return estimatedSSTables;
    }

    public long getExpectedTotalWriteSize()
    {
        return expectedTotalWriteSize;
    }

    public long getLiveSpaceUsedByInProgressCompactions()
    {
        return liveSpaceUsedByInProgressCompactions;
    }

    public static CompactionEstimationWrapper of(long estimatedSSTables, long expectedTotalWriteSize, long liveSpaceUsedByInProgressCompactions)
    {
        return new CompactionEstimationWrapper(estimatedSSTables, expectedTotalWriteSize, liveSpaceUsedByInProgressCompactions);
    }
}
