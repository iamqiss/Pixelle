/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.telemetry.tracing;

import org.density.common.concurrent.RefCountedReleasable;
import org.density.telemetry.Telemetry;
import org.density.telemetry.metrics.MetricsTelemetry;
import org.density.telemetry.metrics.OTelMetricsTelemetry;

import io.opentelemetry.sdk.OpenTelemetrySdk;

/**
 * Otel implementation of Telemetry
 */
public class OTelTelemetry implements Telemetry {

    private final RefCountedReleasable<OpenTelemetrySdk> refCountedOpenTelemetry;

    /**
     * Creates Telemetry instance

     */
    /**
     * Creates Telemetry instance
     * @param refCountedOpenTelemetry open telemetry.
     */
    public OTelTelemetry(RefCountedReleasable<OpenTelemetrySdk> refCountedOpenTelemetry) {
        this.refCountedOpenTelemetry = refCountedOpenTelemetry;
    }

    @Override
    public TracingTelemetry getTracingTelemetry() {
        return new OTelTracingTelemetry<>(refCountedOpenTelemetry, refCountedOpenTelemetry.get().getSdkTracerProvider());
    }

    @Override
    public MetricsTelemetry getMetricsTelemetry() {
        return new OTelMetricsTelemetry<>(refCountedOpenTelemetry, refCountedOpenTelemetry.get().getSdkMeterProvider());
    }
}
