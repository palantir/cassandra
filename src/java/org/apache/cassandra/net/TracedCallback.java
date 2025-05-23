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

package org.apache.cassandra.net;

import java.net.InetAddress;

import com.google.common.collect.ImmutableMap;

import com.palantir.tracing.CloseableSpan;
import com.palantir.tracing.DetachedSpan;

public class TracedCallback<T> implements IAsyncCallbackWithFailure<T>
{
    private final MessageOut message;
    private final IAsyncCallback<T> delegate;
    private final DetachedSpan span;

    public TracedCallback(MessageOut message, IAsyncCallback<T> delegate, DetachedSpan span)
    {
        this.message = message;
        this.delegate = delegate;
        this.span = span;
    }

    @Override
    public void response(MessageIn<T> msg)
    {
        try (CloseableSpan ignored = span.childSpan("TracedCallback#response")) {
            delegate.response(msg);
        }
        span.complete(ImmutableMap.of("verb", message.verb.name()));
    }

    @Override
    public boolean isLatencyForSnitch()
    {
        return delegate.isLatencyForSnitch();
    }

    @Override
    public void onFailure(InetAddress from)
    {
        // Trust that the caller has checked this for us.
        try (CloseableSpan ignored = span.childSpan("TracedCallback#onFailure")) {
            ((IAsyncCallbackWithFailure<?>) delegate).onFailure(from);
        }
        span.complete(ImmutableMap.of("verb", message.verb.name()));
    }
}
