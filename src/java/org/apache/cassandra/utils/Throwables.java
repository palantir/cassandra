/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.utils;

import java.io.IOException;

import com.google.common.base.Optional;
import com.palantir.logsafe.Arg;
import com.palantir.logsafe.SafeArg;
import org.apache.cassandra.FilterExperiment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Throwables
{
    private static final Logger log = LoggerFactory.getLogger(Throwables.class);

    public static Throwable merge(Throwable existingFail, Throwable newFail)
    {
        if (existingFail == null)
            return newFail;
        existingFail.addSuppressed(newFail);
        return existingFail;
    }

    public static void maybeFail(Throwable fail)
    {
        if (failIfCanCast(fail, null))
            throw new RuntimeException(fail);
    }

    public static <T extends Throwable> void maybeFail(Throwable fail, Class<T> checked) throws T
    {
        if (failIfCanCast(fail, checked))
            throw new RuntimeException(fail);
    }

    public static <T extends Throwable> boolean failIfCanCast(Throwable fail, Class<T> checked) throws T
    {
        if (fail == null)
            return false;

        if (fail instanceof Error)
            throw (Error) fail;

        if (fail instanceof RuntimeException)
            throw (RuntimeException) fail;

        if (checked != null && checked.isInstance(fail))
            throw checked.cast(fail);

        return true;
    }

    public static Throwable close(Throwable accumulate, Iterable<? extends AutoCloseable> closeables)
    {
        for (AutoCloseable closeable : closeables)
        {
            try
            {
                closeable.close();
            }
            catch (Throwable t)
            {
                accumulate = merge(accumulate, t);
            }
        }
        return accumulate;
    }

    public static Optional<IOException> extractIOExceptionCause(Throwable t)
    {
        if (t instanceof IOException)
            return Optional.of((IOException) t);
        Throwable cause = t;
        while ((cause = cause.getCause()) != null)
        {
            if (cause instanceof IOException)
                return Optional.of((IOException) cause);
        }
        return Optional.absent();
    }

    public static void assertWithError(boolean condition, Arg<?>... args)
    {
        if (!condition)
        {
            log.error("Assertion failed", (Object[]) args);
            throw new AssertionError();
        }
    }

    public static void assertWithError(boolean condition, String message, Arg<?>... args)
    {
        if (!condition)
        {
            Arg<?>[] newArgs = new Arg[args.length + 1];
            System.arraycopy(args, 0, newArgs, 0, args.length);
            newArgs[args.length] = SafeArg.of("message", message);
            log.error("Assertion failed", (Object[]) newArgs);
            throw new AssertionError(message);
        }
    }
}
