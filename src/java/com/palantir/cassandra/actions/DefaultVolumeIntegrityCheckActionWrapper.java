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

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.UUID;

import com.palantir.cassandra.utils.FileParser;
import org.apache.cassandra.config.DatabaseDescriptor;

public final class DefaultVolumeIntegrityCheckActionWrapper implements Action
{
    private static final String METADATA_NAME = "cassandra-metadata.json";

    private final UUID hostId;

    public DefaultVolumeIntegrityCheckActionWrapper(UUID hostId)
    {
        this.hostId = hostId;
    }

    public void execute()
    {
        String dataDirectory = Arrays
                               .stream(DatabaseDescriptor.getAllDataFileLocations())
                               .findFirst()
                               .orElseThrow(() -> new RuntimeException("No data directory found"));
        String commitLogDirectory = DatabaseDescriptor.getCommitLogLocation();

        Action delegate = new DefaultVolumeIntegrityCheckAction(hostId, withParser(dataDirectory), withParser(commitLogDirectory));
        delegate.execute();
    }

    private FileParser<VolumeMetadata> withParser(String path)
    {
        return new FileParser<>(Paths.get(path, METADATA_NAME), VolumeMetadata.class);
    }
}
