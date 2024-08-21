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

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;


import org.junit.Test;

import com.palantir.cassandra.action.common.CommitLogMetadata;
import com.palantir.cassandra.action.common.DataDriveMetadata;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import org.assertj.core.api.Assertions;

public class VolumeIntegrityCheckActionTest
{
    private static final UUID HOST_1 = UUID.randomUUID();

    private static final UUID HOST_2 = UUID.randomUUID();

    @Test
    public void execute_CommitLogMetadataIfPresentHaveSameHostIdPass()
    {
        Action action = new VolumeIntegrityCheckAction(HOST_1, Optional.empty(), metadataFrom(HOST_1));
        Assertions.assertThatCode(action::execute).doesNotThrowAnyException();
    }

    @Test
    public void execute_CommitLogMetadataIfPresentHaveDifferentHostIdThrows()
    {
        Action action = new VolumeIntegrityCheckAction(HOST_1, Optional.empty(), metadataFrom(HOST_2));
        Assertions.assertThatCode(action::execute).isInstanceOf(SafeIllegalStateException.class);
    }

    @Test
    public void execute_CommitLogIfEmptyDataDriveEmptyPass()
    {
        Action action = new VolumeIntegrityCheckAction(HOST_1, Optional.empty(), Optional.empty());
        Assertions.assertThatCode(action::execute).doesNotThrowAnyException();
    }

    @Test
    public void execute_CommitLogIfEmptyDataDrivePresentThrows()
    {
        Action action = new VolumeIntegrityCheckAction(HOST_1, metadataFrom(HOST_1, Instant.now()), Optional.empty());
        Assertions.assertThatCode(action::execute).isInstanceOf(SafeIllegalStateException.class);
    }

    private Optional<CommitLogMetadata> metadataFrom(UUID hostId)
    {
        return Optional.of(new CommitLogMetadata(hostId));
    }

    private Optional<DataDriveMetadata> metadataFrom(UUID hostId, Instant timestamp)
    {
        return Optional.of(new DataDriveMetadata(hostId, timestamp));
    }
}