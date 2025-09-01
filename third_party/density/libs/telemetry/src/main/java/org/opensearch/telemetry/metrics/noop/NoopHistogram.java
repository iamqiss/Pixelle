/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.telemetry.metrics.noop;

import org.density.common.annotation.InternalApi;
import org.density.telemetry.metrics.Histogram;
import org.density.telemetry.metrics.tags.Tags;

/**
 * No-op {@link Histogram}
 * {@density.internal}
 */
@InternalApi
public class NoopHistogram implements Histogram {

    /**
     * No-op Histogram instance
     */
    public final static NoopHistogram INSTANCE = new NoopHistogram();

    private NoopHistogram() {}

    @Override
    public void record(double value) {

    }

    @Override
    public void record(double value, Tags tags) {

    }
}
