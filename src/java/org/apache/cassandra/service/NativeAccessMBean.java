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
package org.apache.cassandra.service;

import java.io.Serializable;

public interface NativeAccessMBean
{
    boolean isAvailable();

    boolean isMemoryLockable();

    /**
     * After having properly remediated a commitlog corruption including removing the corrupted commitlog file from the
     * node's commitlog_directory, an operator may execute this method to safely re-initialize the CassandraDaemon and
     * StorageService.
     *
     * @throws IllegalNonTransientErrorStateException if StorageService in not in NonTransientError mode or has no
     *          known NonTransientError of the type COMMIT_LOG_CORRUPTION
     */
    void reinitializeFromCommitlogCorruption() throws IllegalNonTransientErrorStateException;

    class IllegalNonTransientErrorStateException extends Exception implements Serializable
    {
        private static final long serialVersionUID = 1068347274649406245L;

        public IllegalNonTransientErrorStateException(String message)
        {
            super(message);
        }
    }
}
