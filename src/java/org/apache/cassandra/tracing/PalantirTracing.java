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

package org.apache.cassandra.tracing;

import com.palantir.tracing.CloseableTracer;
import com.palantir.tracing.Observability;
import com.palantir.tracing.Tracer;
import com.palantir.tracing.Tracers;
import com.palantir.tracing.api.SpanType;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Table;

import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.thrift.TraceMetadata;
import org.apache.thrift.annotation.Nullable;

/**
 * Ported over from UndertowTracing.
 */
public final class PalantirTracing
{
    private static final String PALANTIR_TRACE_ID = "PalantirTraceId";
    private static final String PALANTIR_IS_SAMPLED = "PalantirIsSampled";
    private static final String PALANTIR_PARENT_SPAN_ID = "PalantirParentSpanId";

    private PalantirTracing() {}

    public static void initializeTracerFromIncomingThriftMessage(String thriftOperation, @Nullable TraceMetadata tracing)
    {
        String traceId = Optional.ofNullable(tracing).map(TraceMetadata::getTrace_id).orElseGet(Tracers::randomId);
        Optional<String> parentSpanId = Optional.ofNullable(tracing).map(TraceMetadata::getSpan_id);

        // The typing here is awkward, since we both require a span to be set on the incoming thrift message _and_ the
        // underlying tracer calls eventually converge to handle the field being optional. But what's an extra branch
        // amongst friends...
        if (!parentSpanId.isPresent())
        {
            Tracer.initTraceWithSpan(getObservabilityFromTracing(tracing), traceId, thriftOperation, SpanType.SERVER_INCOMING);
        }
        else
        {
            Tracer.initTraceWithSpan(getObservabilityFromTracing(tracing), traceId, thriftOperation, parentSpanId.get(), SpanType.SERVER_INCOMING);
        }
    }

    public static void initializeTracerFromIncomingMessage(MessageIn<?> message) {
        String traceId = Optional.ofNullable(message.parameters.get(PALANTIR_TRACE_ID)).map(bytes -> new String(bytes, StandardCharsets.UTF_8)).orElseGet(Tracers::randomId);
        Optional<String> parentSpanId = Optional.ofNullable(message.parameters.get(PALANTIR_PARENT_SPAN_ID)).map(bytes -> new String(bytes, StandardCharsets.UTF_8));
        boolean isSampled = Optional.ofNullable(message.parameters.get(PALANTIR_IS_SAMPLED)).map(bytes -> bytes[0] == 1).orElse(false);

        if (parentSpanId.isPresent()) {
            Tracer.initTraceWithSpan(isSampled ? Observability.SAMPLE : Observability.DO_NOT_SAMPLE, traceId, "IN: " + message.verb.toString(), parentSpanId.get(), SpanType.SERVER_INCOMING);
        } else {
            Tracer.initTraceWithSpan(isSampled ? Observability.SAMPLE : Observability.DO_NOT_SAMPLE, traceId, "IN: " + message.verb.toString(), SpanType.SERVER_INCOMING);
        }
    }

    public static void closeServerSpanThrift()
    {
        Tracer.fastCompleteSpan();
    }

    public static void closeServerSpanInterNode()
    {
        Tracer.fastCompleteSpan();
    }

    public static Map<String, byte[]> serializeForMessage() {
        Optional<com.palantir.tracing.TraceMetadata> traceMetadata = Tracer.maybeGetTraceMetadata();
        if (traceMetadata.isPresent()) {
            return ImmutableMap.of(
                PALANTIR_TRACE_ID, traceMetadata.get().getTraceId().getBytes(StandardCharsets.UTF_8),
                PALANTIR_PARENT_SPAN_ID, traceMetadata.get().getSpanId().getBytes(StandardCharsets.UTF_8),
                PALANTIR_IS_SAMPLED, new byte[] { (byte) (Tracer.isTraceObservable() ? 1 : 0) }
            );
        } else {
            return Collections.emptyMap();
        }
    }

    private static Observability getObservabilityFromTracing(@Nullable TraceMetadata tracing)
    {
        return Optional.ofNullable(tracing)
                       .map(TraceMetadata::isIs_sampled)
                       .map(sampled -> sampled ? Observability.SAMPLE : Observability.DO_NOT_SAMPLE)
                       .orElse(Observability.UNDECIDED);
    }
}
