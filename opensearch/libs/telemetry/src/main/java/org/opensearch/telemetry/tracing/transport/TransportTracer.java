/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.telemetry.tracing.transport;

import org.density.common.annotation.ExperimentalApi;
import org.density.telemetry.tracing.Span;
import org.density.telemetry.tracing.SpanCreationContext;

import java.util.Collection;
import java.util.Map;

/**
 * TransportTracer helps in creating a {@link Span} which reads the incoming tracing information
 * from the HTTP or TCP transport headers and propagate the span accordingly.
 * <p>
 * All methods on the Tracer object are multi-thread safe.
 *
 * @density.experimental
 */
@ExperimentalApi
public interface TransportTracer {
    /**
     * Start the span with propagating the tracing info from the HttpRequest header.
     *
     * @param spanCreationContext span name.
     * @param headers transport headers
     * @return the span instance
     */
    Span startSpan(SpanCreationContext spanCreationContext, Map<String, Collection<String>> headers);
}
