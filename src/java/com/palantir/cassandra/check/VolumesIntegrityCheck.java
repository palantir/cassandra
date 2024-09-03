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

package com.palantir.cassandra.check;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;

import com.google.common.annotations.VisibleForTesting;

import com.fasterxml.jackson.core.type.TypeReference;
import com.palantir.cassandra.utils.FileParser;
import com.palantir.logsafe.Preconditions;
import org.apache.cassandra.config.DatabaseDescriptor;

/**
 * Check if the backing volume has been tampered with. While Cassandra supports multiple data drive, we only
 * use 1 data drive.
 */
public final class VolumesIntegrityCheck
{
    public static final String VOLUME_METADATA_NAME = "cassandra-metadata.json";

    private final UUID hostId;

    private final FileParser<VolumeMetadata> dataDriveMetadataFileParser;

    private final FileParser<VolumeMetadata> commitLogMetadataFileParser;

    public VolumesIntegrityCheck(UUID hostId)
    {
        this(hostId, withParser(getDataDirectory()), withParser(DatabaseDescriptor.getCommitLogLocation()));
    }

    @VisibleForTesting
    VolumesIntegrityCheck(UUID hostId,
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

            /**
             * Write order matters here based on the implementation for {@link com.palantir.cassandra.check.VolumesIntegrityCheck#check}.
             * Write to the data drive specifies that we've committed the metadata files to the node. Without writing
             * to the data drive, we can't differentiate if this is a new node to the cluster or the data drive has
             * been swapped.
             */
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
        if (!present) parser.write(VolumeMetadata.of(hostId));
    }

    private static FileParser<VolumeMetadata> withParser(String path)
    {
        return new FileParser<>(Paths.get(path, VOLUME_METADATA_NAME), new TypeReference<VolumeMetadata>()
        {
        });
    }

    private static String getDataDirectory()
    {
        return Arrays
               .stream(DatabaseDescriptor.getAllDataFileLocations())
               .findFirst()
               .orElseThrow(() -> new RuntimeException("No data directory found"));
    }
}
