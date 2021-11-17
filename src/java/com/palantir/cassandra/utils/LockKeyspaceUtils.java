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

package com.palantir.cassandra.utils;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;

public class LockKeyspaceUtils
{
    private static final Logger log = LoggerFactory.getLogger(LockKeyspaceUtils.class);
    private static final String CREATE_FILENAME = "lock_keyspace_creation";

    public static synchronized void lockKeyspaceCreation() throws IOException {
        File lockFile = new File(DatabaseDescriptor.getLockKeyspacesDirectory(), CREATE_FILENAME);
        try
        {
            lockFile.createNewFile();
        }
        catch (IOException e)
        {
            log.warn("Cannot create lock keyspaces file.", lockFile.getAbsolutePath(), e);
            throw e;
        }
    }

    public static synchronized void unlockKeyspaceCreation() {
        File lockFile = new File(DatabaseDescriptor.getLockKeyspacesDirectory(), CREATE_FILENAME);
        lockFile.delete();
    }

    public static synchronized void validateKeyspaceCreationUnlocked() {
        if (getCreateLockFile().exists())
            throw new org.apache.cassandra.exceptions.InvalidRequestException("keyspace creation is disabled");
    }

    private static synchronized File getCreateLockFile() {
        return new File(DatabaseDescriptor.getLockKeyspacesDirectory(), CREATE_FILENAME);
    }
}
