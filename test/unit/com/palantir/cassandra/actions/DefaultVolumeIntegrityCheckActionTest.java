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
import java.lang.reflect.Field;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import com.palantir.cassandra.utils.FileParser;
import org.assertj.core.api.Assertions;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;

public final class DefaultVolumeIntegrityCheckActionTest
{
    private static final UUID HOST_1 = UUID.randomUUID();

    private static final UUID HOST_2 = UUID.randomUUID();

    private static final String POD_NAME_1 = "pod-1";

    private static final String POD_NAME_2 = "pod-2";

    private FileParser<VolumeMetadata> dataDriveMetadataFileParser;

    private FileParser<VolumeMetadata> commitLogMetadataFileParser;

    private Action action;

    @Before
    public void beforeEach()
    {
        withMutableEnv().put(VolumeMetadata.POD_NAME_ENV, POD_NAME_1);
        dataDriveMetadataFileParser = mock(FileParser.class);
        commitLogMetadataFileParser = mock(FileParser.class);
        action =  new DefaultVolumeIntegrityCheckAction(HOST_1, dataDriveMetadataFileParser, commitLogMetadataFileParser);
    }

    @Test
    public void execute_commitLogIfPresentHaveSameHostIdPass()
    {
        mockParserRead(dataDriveMetadataFileParser, Optional.empty());
        mockParserRead(commitLogMetadataFileParser, volumeMetadataFrom(HOST_1));
        Assertions.assertThatCode(action::execute).doesNotThrowAnyException();
    }

    @Test
    public void execute_commitLogIfPresentHaveDifferentHostIdThrows()
    {
        mockParserRead(dataDriveMetadataFileParser, Optional.empty());
        mockParserRead(commitLogMetadataFileParser, volumeMetadataFrom(HOST_2));
        Assertions.assertThatCode(action::execute).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void execute_commitLogIfPresentHaveSamePodNameEnvPass()
    {
        mockParserRead(dataDriveMetadataFileParser, Optional.empty());
        mockParserRead(commitLogMetadataFileParser, volumeMetadataFrom(HOST_1));
        Assertions.assertThatCode(action::execute).doesNotThrowAnyException();
    }

    @Test
    public void execute_commitLogIfPresentHaveDifferentPodNameEnvThrows()
    {
        mockParserRead(dataDriveMetadataFileParser, Optional.empty());
        mockParserRead(commitLogMetadataFileParser, volumeMetadataFrom(HOST_1));
        withMutableEnv().put(VolumeMetadata.POD_NAME_ENV, POD_NAME_2);
        Assertions.assertThatCode(action::execute).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void execute_commitLogIfNotPresentEmptyDataDrivePass()
    {
        mockParserRead(dataDriveMetadataFileParser, Optional.empty());
        mockParserRead(commitLogMetadataFileParser, Optional.empty());
        Assertions.assertThatCode(action::execute).doesNotThrowAnyException();
    }

    @Test
    public void execute_commitLogIfNotPresentNonEmptyDataDriveThrows()
    {
        mockParserRead(dataDriveMetadataFileParser, volumeMetadataFrom(HOST_1));
        mockParserRead(commitLogMetadataFileParser, Optional.empty());
        Assertions.assertThatCode(action::execute).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void execute_onlyWriteWhenEmpty() throws IOException
    {
        mockParserRead(dataDriveMetadataFileParser, Optional.empty());
        mockParserRead(commitLogMetadataFileParser, Optional.empty());
        action.execute();
        verify(dataDriveMetadataFileParser, times(1)).write(VolumeMetadata.of(HOST_1));
        verify(commitLogMetadataFileParser, times(1)).write(VolumeMetadata.of(HOST_1));
    }

    private static void mockParserRead(FileParser<VolumeMetadata> parser, Optional<VolumeMetadata> metadata)
    {
        try
        {
            when(parser.read()).thenReturn(metadata);
        }
        catch (Exception exception)
        {
            throw new RuntimeException(exception);
        }
    }

    @SuppressWarnings("unchecked") // Supress type cast from System.getenv
    private static Map<String, String> withMutableEnv()
    {
        try
        {
            // Use reflection here to mutate env since System is a final class
            Class<?> cls = System.getenv().getClass();
            Field field = cls.getDeclaredField("m");
            field.setAccessible(true);
            return (Map<String, String>) field.get(System.getenv());
        }
        catch (Exception _unused)
        {
            throw new RuntimeException();
        }
    }

    private static Optional<VolumeMetadata> volumeMetadataFrom(UUID hostId)
    {
        return Optional.of(VolumeMetadata.of(hostId));
    }
}
