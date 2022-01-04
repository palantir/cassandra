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

package com.palantir.cassandra.settings;

import java.io.IOException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Test;

import org.apache.cassandra.exceptions.InvalidRequestException;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class LockKeyspaceCreationSettingTest
{

    @After
    public void cleanup() {
        LockKeyspaceCreationSetting.instance.setFalse();
    }

    @Test
    public void validateKeyspaceCreationUnlocked_throwsWhenLocked() throws IOException
    {
        LockKeyspaceCreationSetting setting = LockKeyspaceCreationSetting.instance;
        setting.setTrue();
        assertThatThrownBy(setting::validateKeyspaceCreationUnlocked)
            .isInstanceOf(InvalidRequestException.class)
            .hasMessage("keyspace creation is disabled");
    }

    @Test
    public void validateKeyspaceCreationUnlocked_doesNotThrowWhenUnlocked() throws IOException
    {
        LockKeyspaceCreationSetting setting = LockKeyspaceCreationSetting.instance;
        setting.setTrue();
        setting.setFalse();
        setting.validateKeyspaceCreationUnlocked();
    }
}
