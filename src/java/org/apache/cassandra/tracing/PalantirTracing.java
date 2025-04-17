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

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.logsafe.SafeArg;
import com.palantir.tracing.Observability;
import com.palantir.tracing.Tracer;
import com.palantir.tracing.Tracers;
import com.palantir.tracing.api.SpanType;
import org.apache.cassandra.thrift.TraceMetadata;
import org.apache.thrift.annotation.Nullable;

/**
 * Ported over from UndertowTracing.
 */
public final class PalantirTracing
{
    private static final Logger logger = LoggerFactory.getLogger(PalantirTracing.class);
    private PalantirTracing() {}

    public static void initializeTracerFromIncomingThriftMessage(String thriftOperation, @Nullable TraceMetadata tracing)
    {
        String traceId = Optional.ofNullable(tracing)
                                 .map(TraceMetadata::getTrace_id)
                                 .orElseGet(Tracers::randomId);
        Optional<String> parentSpanId = Optional.ofNullable(tracing)
                                                .map(TraceMetadata::getSpan_id);

        logger.info("raw tracing metadata",
                    SafeArg.of("tracing", tracing),
                    SafeArg.of("traceId", traceId),
                    SafeArg.of("parentSpanId", parentSpanId));
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

    public static void closeServerSpanThrift()
    {
        Tracer.fastCompleteSpan();
    }

    private static Observability getObservabilityFromTracing(@Nullable TraceMetadata tracing)
    {
        return Optional.ofNullable(tracing)
                       .map(TraceMetadata::isIs_sampled)
                       .map(sampled -> sampled ? Observability.SAMPLE : Observability.DO_NOT_SAMPLE)
                       .orElse(Observability.UNDECIDED);
    }
}