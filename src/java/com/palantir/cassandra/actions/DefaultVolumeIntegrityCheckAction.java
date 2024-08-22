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

package com.palantir.cassandra.actions;

import java.util.Optional;
import java.util.UUID;

import com.palantir.logsafe.Preconditions;

/**
 * Check if the backing volume has been tampered with. While Cassandra supports multiple data drive, we only
 * use 1 data drive.
 */
public final class VolumeIntegrityCheckAction implements Action
{
    private static final int MAX_EXPECTED_DATA_DRIVE = 1;

    private final UUID hostId;

    private final Optional<VolumeMetadata> maybeDataDriveMetadata;

    private final Optional<VolumeMetadata> maybeCommitLogMetadata;

    public VolumeIntegrityCheckAction(UUID hostId,
                                      Optional<VolumeMetadata> maybeDataDriveMetadata,
                                      Optional<VolumeMetadata> maybeCommitLogMetadata)
    {
        this.hostId = hostId;
        this.maybeDataDriveMetadata = maybeDataDriveMetadata;
        this.maybeCommitLogMetadata = maybeCommitLogMetadata;
    }

    public void execute()
    {
        if (maybeCommitLogMetadata.isPresent())
        {
            VolumeMetadata commitLogMetadata = maybeCommitLogMetadata.get();
            Preconditions.checkState(hostId.equals(commitLogMetadata.getHostId()),
                                     "Expected SystemKeyspace HostId to match with commitlog");
            Preconditions.checkState(VolumeMetadata.of(hostId).equals(commitLogMetadata));
        }
        else
        {
            Preconditions.checkState(maybeDataDriveMetadata.isEmpty(),
                                     "Expected no metadata has been written to data drive if commitlog is empty");
        }
    }
}
