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
import org.density.telemetry.metrics.Histogram;
import org.density.telemetry.metrics.MetricsRegistry;
import org.density.telemetry.metrics.TaggedMeasurement;
import org.density.telemetry.metrics.tags.Tags;

import java.io.Closeable;
import java.io.IOException;
import java.util.function.Supplier;

/**
 *No-op {@link MetricsRegistry}
 * {@density.internal}
 */
@InternalApi
public class NoopMetricsRegistry implements MetricsRegistry {

    /**
     * No-op Meter instance
     */
    public final static NoopMetricsRegistry INSTANCE = new NoopMetricsRegistry();

    private NoopMetricsRegistry() {}

    @Override
    public Counter createCounter(String name, String description, String unit) {
        return NoopCounter.INSTANCE;
    }

    @Override
    public Counter createUpDownCounter(String name, String description, String unit) {
        return NoopCounter.INSTANCE;
    }

    @Override
    public Histogram createHistogram(String name, String description, String unit) {
        return NoopHistogram.INSTANCE;
    }

    @Override
    public Closeable createGauge(String name, String description, String unit, Supplier<Double> valueProvider, Tags tags) {
        return () -> {};
    }

    @Override
    public Closeable createGauge(String name, String description, String unit, Supplier<TaggedMeasurement> value) {
        return () -> {};
    }

    @Override
    public void close() throws IOException {

    }
}
