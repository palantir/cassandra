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

import org.junit.Test;

import org.apache.cassandra.io.ExceededDiskThresholdException;
import org.apache.cassandra.io.FSErrorHandler;
import org.apache.cassandra.io.util.FileUtils;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class DefaultFSErrorHandlerTest
{
    @Test
    public void testHandleExceededDiskThresholdIsInvoked() throws InterruptedException
    {
        FSErrorHandler spyHandler = spy(new DefaultFSErrorHandler());
        FileUtils.setFSErrorHandler(spyHandler);
        FileUtils.setDefaultUncaughtExceptionHandler();

        Thread testThread = new Thread(() -> {
            throw new ExceededDiskThresholdException(null, 0, 0);
        });
        testThread.start();
        testThread.join();

        verify(spyHandler).handleExceededDiskThreshold(any());
    }

    @Test
    public void testHandleExceededDiskDoesNotRecordErrorAfterNodeDisabled() throws InterruptedException
    {
        DefaultFSErrorHandler spyHandler = spy(new DefaultFSErrorHandler());
        FileUtils.setFSErrorHandler(spyHandler);
        FileUtils.setDefaultUncaughtExceptionHandler();
        StorageService.instance.disableNode();

        Thread testThread = new Thread(() -> {
            throw new ExceededDiskThresholdException(null, 0, 0);
        });
        testThread.start();
        testThread.join();

        verify(spyHandler, never()).handleExceededDiskThresholdInternal(any());
    }
}
