/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.telemetry.tracing;

/**
 * Propagated span through context propagation
 */
public class OTelPropagatedSpan extends OTelSpan {

    /**
     * Creates OTelPropagatedSpan
     * @param span otel propagated span
     */
    public OTelPropagatedSpan(io.opentelemetry.api.trace.Span span) {
        super(null, span, null);
    }
}
