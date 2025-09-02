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

/**
 * No-op implementation of SpanScope
 *
 * @density.internal
 */
@InternalApi
public final class NoopScopedSpan implements ScopedSpan {

    /**
     * No-args constructor
     */
    public NoopScopedSpan() {}

    @Override
    public void addAttribute(String key, String value) {

    }

    @Override
    public void addAttribute(String key, long value) {

    }

    @Override
    public void addAttribute(String key, double value) {

    }

    @Override
    public void addAttribute(String key, boolean value) {

    }

    @Override
    public void addEvent(String event) {

    }

    @Override
    public void setError(Exception exception) {

    }

    @Override
    public void close() {

    }
}
