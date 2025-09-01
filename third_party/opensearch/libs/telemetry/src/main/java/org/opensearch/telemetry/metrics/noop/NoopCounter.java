/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.telemetry.metrics.noop;

import org.density.common.annotation.InternalApi;
import org.density.telemetry.metrics.Counter;
import org.density.telemetry.metrics.tags.Tags;

/**
 * No-op {@link Counter}
 * {@density.internal}
 */
@InternalApi
public class NoopCounter implements Counter {

    /**
     * No-op Counter instance
     */
    public final static NoopCounter INSTANCE = new NoopCounter();

    private NoopCounter() {}

    @Override
    public void add(double value) {

    }

    @Override
    public void add(double value, Tags tags) {

    }
}
