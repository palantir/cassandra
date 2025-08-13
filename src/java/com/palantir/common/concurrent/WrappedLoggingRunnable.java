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

package com.palantir.common.concurrent;

import com.google.common.base.Throwables;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.repair.RepairRunnable;
import org.apache.cassandra.utils.WrappedRunnable;

public abstract class WrappedLoggingRunnable implements Runnable
{

    private static final Logger logger = LoggerFactory.getLogger(WrappedLoggingRunnable.class);

    public final void run()
    {
        try
        {
            runMayThrow();
        }
        catch (Throwable t)
        {
            logger.error("Exception was thrown from runnable, logging and propagating.", t);
            throw Throwables.propagate(t);
        }
    }

    abstract protected void runMayThrow() throws Exception;
}
