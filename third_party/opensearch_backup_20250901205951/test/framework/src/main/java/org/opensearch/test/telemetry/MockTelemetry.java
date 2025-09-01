/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.test.telemetry;

import org.density.telemetry.Telemetry;
import org.density.telemetry.TelemetrySettings;
import org.density.telemetry.metrics.Counter;
import org.density.telemetry.metrics.Histogram;
import org.density.telemetry.metrics.MetricsTelemetry;
import org.density.telemetry.metrics.TaggedMeasurement;
import org.density.telemetry.metrics.noop.NoopCounter;
import org.density.telemetry.metrics.noop.NoopHistogram;
import org.density.telemetry.metrics.tags.Tags;
import org.density.telemetry.tracing.TracingTelemetry;
import org.density.test.telemetry.tracing.MockTracingTelemetry;

import java.io.Closeable;
import java.util.function.Supplier;

/**
 * Mock {@link Telemetry} implementation for testing.
 */
public class MockTelemetry implements Telemetry {
    /**
     * Constructor with settings.
     * @param settings telemetry settings.
     */
    public MockTelemetry(TelemetrySettings settings) {

    }

    @Override
    public TracingTelemetry getTracingTelemetry() {
        return new MockTracingTelemetry();
    }

    @Override
    public MetricsTelemetry getMetricsTelemetry() {
        return new MetricsTelemetry() {
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
            public void close() {

            }
        };
    }
}
