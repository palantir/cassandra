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

import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public final class VolumeMetadata
{
    public static final String POD_NAME_ENV = "POD_NAME";

    private final UUID hostId;

    private final String podName;

    public VolumeMetadata(UUID hostId)
    {
        this(hostId, Optional.ofNullable(System.getenv(POD_NAME_ENV)).orElse(""));
    }

    @JsonCreator
    public VolumeMetadata(@JsonProperty("host-id") UUID hostId, @JsonProperty("pod-name") String podName)
    {
        this.hostId = hostId;
        this.podName = podName;
    }

    public static VolumeMetadata of(UUID hostId)
    {
        return new VolumeMetadata(hostId);
    }

    @Override
    public String toString()
    {
        return String.format("VolumeMetadata{hostId=%s, podName=%s}", hostId, podName);
    }

    @Override
    public boolean equals(Object other)
    {
        if (!(other instanceof VolumeMetadata))
        {
            return false;
        }
        VolumeMetadata volumeMetadata = (VolumeMetadata) other;
        return hostId.equals(volumeMetadata.hostId) && podName.equals(volumeMetadata.podName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(hostId, podName);
    }
}
