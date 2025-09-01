/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.telemetry.tracing;

import org.density.common.annotation.ExperimentalApi;

import java.io.Closeable;

/**
 * Interface for tracing telemetry providers
 *
 * @density.experimental
 */
@ExperimentalApi
public interface TracingTelemetry extends Closeable {

    /**
     * Creates span with provided arguments
     *
     * @param spanCreationContext span creation context.
     * @param parentSpan parent span.
     * @return span instance
     */
    Span createSpan(SpanCreationContext spanCreationContext, Span parentSpan);

    /**
     * provides tracing context propagator
     * @return tracing context propagator instance
     */
    TracingContextPropagator getContextPropagator();

}
