/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.test.telemetry.tracing;

import org.density.telemetry.tracing.Span;
import org.density.telemetry.tracing.SpanCreationContext;
import org.density.telemetry.tracing.TracingContextPropagator;
import org.density.telemetry.tracing.TracingTelemetry;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Mock {@link TracingTelemetry} implementation for testing.
 */
public class MockTracingTelemetry implements TracingTelemetry {

    private final SpanProcessor spanProcessor = new StrictCheckSpanProcessor();
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    /**
     * Base constructor.
     */
    public MockTracingTelemetry() {}

    @Override
    public Span createSpan(SpanCreationContext spanCreationContext, Span parentSpan) {
        Span span = new MockSpan(spanCreationContext, parentSpan, spanProcessor);
        if (shutdown.get() == false) {
            spanProcessor.onStart(span);
        }
        return span;
    }

    @Override
    public TracingContextPropagator getContextPropagator() {
        return new MockTracingContextPropagator(spanProcessor);
    }

    @Override
    public void close() {
        shutdown.set(true);
    }

}
