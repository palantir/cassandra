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

import java.io.File;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DisallowedDirectories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.ExceededDiskThresholdException;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.io.FSErrorHandler;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.utils.JVMStabilityInspector;

public class DefaultFSErrorHandler implements FSErrorHandler
{
    private static final Logger logger = LoggerFactory.getLogger(DefaultFSErrorHandler.class);

    @Override
    public void handleCorruptSSTable(CorruptSSTableException e)
    {
        if (!StorageService.instance.isSetupCompleted())
            handleStartupFSError(e);

        JVMStabilityInspector.inspectThrowable(e);
        switch (DatabaseDescriptor.getDiskFailurePolicy())
        {
            case stop:
                recordError(StorageServiceMBean.NonTransientError.SSTABLE_CORRUPTION, e.path);
                logger.error("Encountered corrupt sstable exception, not stopping transports due to disk failure policy \"{}\"",
                             DatabaseDescriptor.getDiskFailurePolicy(),
                             e);
                break;
            case stop_paranoid_always:
            case stop_paranoid:
                recordErrorAndDisableNode(StorageServiceMBean.NonTransientError.SSTABLE_CORRUPTION, e.path);
                logger.error("Stopping transports and compaction due to corrupt sstable exception, disk failure policy \"{}\"",
                             DatabaseDescriptor.getDiskFailurePolicy(),
                             e);
                break;
        }
    }

    @Override
    public void handleExceededDiskThreshold(ExceededDiskThresholdException e) {
        if (!StorageService.instance.isSetupCompleted())
            handleStartupFSError(e);

        JVMStabilityInspector.inspectThrowable(e);
        if (StorageService.instance.isNodeDisabled()) {
            logger.debug("Node already disabled. Ignoring ExceededDiskThresholdException");
            return;
        }
        handleExceededDiskThresholdInternal(e);
    }

    @VisibleForTesting
    void handleExceededDiskThresholdInternal(ExceededDiskThresholdException e) {
        switch (DatabaseDescriptor.getDiskFailurePolicy())
        {
            case stop:
                recordError(StorageServiceMBean.NonTransientError.EXCEEDED_DISK_THRESHOLD, e.file);
                logger.error("Encountered exceeded disk threshold exception, not stopping transports due to disk failure policy \"{}\"",
                             DatabaseDescriptor.getDiskFailurePolicy(),
                             e);
                break;
            case stop_paranoid_always:
            case stop_paranoid:
                recordErrorAndDisableNode(StorageServiceMBean.NonTransientError.EXCEEDED_DISK_THRESHOLD, e.file);
                logger.error("Stopping transports and compaction due to exceeded disk threshold exception. Disk failure policy: \"{}\"",
                             DatabaseDescriptor.getDiskFailurePolicy(),
                             e);
                break;
        }
    }

    @Override
    public void handleFSError(FSError e)
    {
        if (!StorageService.instance.isSetupCompleted())
            handleStartupFSError(e);

        JVMStabilityInspector.inspectThrowable(e);
        switch (DatabaseDescriptor.getDiskFailurePolicy())
        {
            case stop_paranoid:
            case stop_paranoid_always:
            case stop:
                recordErrorAndDisableNode(StorageServiceMBean.NonTransientError.FS_ERROR, e.path);
                logger.error("Stopping transports and compaction due to file system exception, disk failure policy \"{}\"",
                             DatabaseDescriptor.getDiskFailurePolicy(),
                             e);
                break;
            case best_effort:
                // for both read and write errors mark the path as unwritable.
                DisallowedDirectories.maybeMarkUnwritable(e.path);
                if (e instanceof FSReadError)
                {
                    File directory = DisallowedDirectories.maybeMarkUnreadable(e.path);
                    if (directory != null)
                        Keyspace.removeUnreadableSSTables(directory);
                }
                break;
            case ignore:
                // already logged, so left nothing to do
                break;
            default:
                throw new IllegalStateException();
        }
    }

    private static void handleStartupFSError(Throwable t)
    {
        switch (DatabaseDescriptor.getDiskFailurePolicy())
        {
            case stop_paranoid:
            case stop:
            case die:
                logger.error("Exiting forcefully due to file system exception on startup, disk failure policy \"{}\"",
                             DatabaseDescriptor.getDiskFailurePolicy(),
                             t);
                JVMStabilityInspector.killCurrentJVM(t, true);
                break;
            default:
                // includes stop_paranoid_on_startup
                break;
        }
    }

    private static void recordErrorAndDisableNode(StorageServiceMBean.NonTransientError error, File path) {
        recordError(error, path);
        StorageService.instance.disableNode();
    }

    private static void recordError(StorageServiceMBean.NonTransientError error, File path) {
        StorageService.instance.recordNonTransientError(error, ImmutableMap.of("path", path.toString()));
    }
}
