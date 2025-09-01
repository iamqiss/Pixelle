/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.telemetry.tracing.runnable;

import org.density.telemetry.tracing.ScopedSpan;
import org.density.telemetry.tracing.SpanCreationContext;
import org.density.telemetry.tracing.Tracer;

/**
 * Wraps the runnable and add instrumentation to trace the {@link Runnable}
 */
public class TraceableRunnable implements Runnable {
    private final Runnable runnable;
    private final SpanCreationContext spanCreationContext;
    private final Tracer tracer;

    /**
     * Constructor.
     * @param tracer tracer
     * @param spanCreationContext spanCreationContext
     * @param runnable runnable.
     */
    public TraceableRunnable(Tracer tracer, SpanCreationContext spanCreationContext, Runnable runnable) {
        this.tracer = tracer;
        this.spanCreationContext = spanCreationContext;
        this.runnable = runnable;
    }

    @Override
    public void run() {
        try (ScopedSpan spanScope = tracer.startScopedSpan(spanCreationContext)) {
            runnable.run();
        }
    }
}
