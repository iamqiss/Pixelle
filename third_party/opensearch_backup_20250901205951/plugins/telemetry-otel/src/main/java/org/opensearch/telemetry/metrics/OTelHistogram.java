/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.telemetry.metrics;

import org.density.telemetry.OTelAttributesConverter;
import org.density.telemetry.metrics.tags.Tags;

import io.opentelemetry.api.metrics.DoubleHistogram;

/**
 * OTel aware implementation {@link Histogram}
 */
class OTelHistogram implements Histogram {

    private final DoubleHistogram otelDoubleHistogram;

    /**
     * Constructor
     * @param otelDoubleCounter delegate counter.
     */
    public OTelHistogram(DoubleHistogram otelDoubleCounter) {
        this.otelDoubleHistogram = otelDoubleCounter;
    }

    @Override
    public void record(double value) {
        otelDoubleHistogram.record(value);
    }

    @Override
    public void record(double value, Tags tags) {
        otelDoubleHistogram.record(value, OTelAttributesConverter.convert(tags));
    }
}
