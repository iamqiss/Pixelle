/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.telemetry.tracing.noop;

import org.density.common.annotation.InternalApi;
import org.density.telemetry.tracing.Span;
import org.density.telemetry.tracing.SpanScope;

/**
 * No-op implementation of {@link SpanScope}
 *
 * @density.internal
 */
@InternalApi
public class NoopSpanScope implements SpanScope {
    /**
     * Constructor.
     */
    public NoopSpanScope() {

    }

    @Override
    public void close() {

    }

    @Override
    public SpanScope attach() {
        return this;
    }

    @Override
    public Span getSpan() {
        return NoopSpan.INSTANCE;
    }
}
