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

package com.palantir.cassandra.action;

import java.util.Optional;
import java.util.UUID;

import com.palantir.cassandra.action.common.CommitLogMetadata;
import com.palantir.cassandra.action.common.DataDriveMetadata;
import com.palantir.logsafe.Preconditions;

public final class VolumeIntegrityCheckAction implements Action
{
    private final UUID hostId;

    private final Optional<DataDriveMetadata> maybeDataDriveMetadata;

    private final Optional<CommitLogMetadata> maybeCommitLogMetadata;

    public VolumeIntegrityCheckAction(
    UUID hostId,
    Optional<DataDriveMetadata> maybeDataDriveMetadata,
    Optional<CommitLogMetadata> maybeCommitLogMetadata)
    {
        this.hostId = hostId;
        this.maybeDataDriveMetadata = maybeDataDriveMetadata;
        this.maybeCommitLogMetadata = maybeCommitLogMetadata;
    }

    public void execute()
    {
        if (maybeCommitLogMetadata.isPresent())
        {
            Preconditions.checkState(hostId.equals(maybeCommitLogMetadata.get().hostId));
        }
        else
        {
            Preconditions.checkState(!maybeDataDriveMetadata.isPresent());
        }
    }
}
