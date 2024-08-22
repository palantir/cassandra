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

import java.io.IOException;
import java.util.Optional;
import java.util.UUID;

import com.palantir.cassandra.utils.FileParser;
import com.palantir.logsafe.Preconditions;

/**
 * Check if the backing volume has been tampered with. While Cassandra supports multiple data drive, we only
 * use 1 data drive.
 */
public final class DefaultVolumeIntegrityCheckAction implements Action
{
    private final UUID hostId;

    private final FileParser<VolumeMetadata> dataDriveMetadataFileParser;

    private final FileParser<VolumeMetadata> commitLogMetadataFileParser;

    public DefaultVolumeIntegrityCheckAction(UUID hostId,
                                             FileParser<VolumeMetadata> dataDriveMetadataFileParser,
                                             FileParser<VolumeMetadata> commitLogMetadataFileParser)
    {
        this.hostId = hostId;
        this.dataDriveMetadataFileParser = dataDriveMetadataFileParser;
        this.commitLogMetadataFileParser = commitLogMetadataFileParser;
    }

    public void execute()
    {
        try
        {
            Optional<VolumeMetadata> maybeDataDriveMetadata = dataDriveMetadataFileParser.read();
            Optional<VolumeMetadata> maybeCommitLogMetadata = commitLogMetadataFileParser.read();
            check(maybeDataDriveMetadata, maybeCommitLogMetadata);

            // Write order matters here. We treat write to the data drive as
            // a "locking" operation, i.e the node is in a steady state.
            writeIfEmpty(commitLogMetadataFileParser, maybeCommitLogMetadata.isPresent());
            writeIfEmpty(dataDriveMetadataFileParser, maybeDataDriveMetadata.isPresent());
        }
        catch (IOException cause)
        {
            throw new RuntimeException("Fail to execute check for volume integrity", cause);
        }
    }

    private void check(Optional<VolumeMetadata> maybeDataDriveMetadata, Optional<VolumeMetadata> maybeCommitLogMetadata)
    {
        if (maybeCommitLogMetadata.isPresent())
        {
            Preconditions.checkState(VolumeMetadata.of(hostId).equals(maybeCommitLogMetadata.get()),
                                     "Expected SystemKeyspace HostId and POD_NAME to match with commitlog");
        }
        else
        {
            Preconditions.checkState(!maybeDataDriveMetadata.isPresent(),
                                     "Expected no metadata has been written to data drive if commitlog is empty");
        }
    }

    private void writeIfEmpty(FileParser<VolumeMetadata> parser, boolean present) throws IOException
    {
        if (!present)
        {
            parser.write(VolumeMetadata.of(hostId));
        }
    }
}
