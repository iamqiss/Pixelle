/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.telemetry.tracing.noop;

import org.density.common.annotation.InternalApi;
import org.density.telemetry.tracing.ScopedSpan;
import org.density.telemetry.tracing.Span;
import org.density.telemetry.tracing.SpanContext;
import org.density.telemetry.tracing.SpanCreationContext;
import org.density.telemetry.tracing.SpanScope;
import org.density.telemetry.tracing.Tracer;

import java.util.Collection;
import java.util.Map;

/**
 * No-op implementation of Tracer
 *
 * @density.internal
 */
@InternalApi
public class NoopTracer implements Tracer {

    /**
     * No-op Tracer instance
     */
    public static final Tracer INSTANCE = new NoopTracer();

    private NoopTracer() {}

    @Override
    public Span startSpan(SpanCreationContext context) {
        return NoopSpan.INSTANCE;
    }

    @Override
    public SpanContext getCurrentSpan() {
        return new SpanContext(NoopSpan.INSTANCE);
    }

    @Override
    public ScopedSpan startScopedSpan(SpanCreationContext spanCreationContext) {
        return ScopedSpan.NO_OP;
    }

    @Override
    public SpanScope withSpanInScope(Span span) {
        return SpanScope.NO_OP;
    }

    @Override
    public boolean isRecording() {
        return false;
    }

    @Override
    public void close() {

    }

    @Override
    public Span startSpan(SpanCreationContext spanCreationContext, Map<String, Collection<String>> header) {
        return NoopSpan.INSTANCE;
    }
}
