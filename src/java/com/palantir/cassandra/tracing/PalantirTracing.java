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

package com.palantir.cassandra.tracing;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import com.google.common.base.Strings;

import com.palantir.tracing.DetachedSpan;
import com.palantir.tracing.Observability;
import com.palantir.tracing.Tracers;
import com.palantir.tracing.api.SpanType;
import com.palantir.tracing.api.TraceHttpHeaders;
import org.apache.cassandra.net.MessageIn;

/**
 * Ported over from UndertowTracing.
 */
public final class PalantirTracing
{
    private static final String DEFAULT_OPERATION_NAME = "Messaging Service: receive";

    private PalantirTracing()
    {
    }

    public static DetachedSpan initializeFromIncomingRpcServerIncoming(MessageIn message)
    {
        byte[] maybeTraceId = (byte[]) message.parameters.get(TraceHttpHeaders.TRACE_ID);
        boolean newTraceId = maybeTraceId == null;
        String traceId = newTraceId ? Tracers.randomId() : new String(maybeTraceId, StandardCharsets.UTF_8);
        return DetachedSpan.start(
            getObservabilityFromHeader(message),
            traceId,
            newTraceId ? Optional.empty() : getSpanIdFromHeader(message),
            DEFAULT_OPERATION_NAME,
            SpanType.SERVER_INCOMING);
    }

    /**
     * Force sample iff the context contains a "1" X-B3-Sampled header, force not sample if the header contains another
     * non-empty value, or undecided if there is no such header or the header is empty.
     */
    private static Observability getObservabilityFromHeader(MessageIn message)
    {
        String header = getHeaderFromMessage(message, TraceHttpHeaders.IS_SAMPLED).orElse("");
        if (Strings.isNullOrEmpty(header))
        {
            return Observability.UNDECIDED;
        }
        else
        {
            return "1".equals(header) ? Observability.SAMPLE : Observability.DO_NOT_SAMPLE;
        }
    }

    private static Optional<String> getSpanIdFromHeader(MessageIn message)
    {
        return getHeaderFromMessage(message, TraceHttpHeaders.SPAN_ID);
    }

    private static Optional<String> getHeaderFromMessage(MessageIn message, String headerName)
    {
        byte[] maybeHeaderValue = (byte[]) message.parameters.get(headerName);
        if (maybeHeaderValue != null)
        {
            return Optional.of(new String(maybeHeaderValue, StandardCharsets.UTF_8));
        }
        return Optional.empty();
    }
}
