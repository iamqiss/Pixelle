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

import io.opentelemetry.api.metrics.DoubleUpDownCounter;

/**
 * OTel Counter
 */
public class OTelUpDownCounter implements Counter {

    private final DoubleUpDownCounter doubleUpDownCounter;

    /**
     * Constructor
     * @param doubleUpDownCounter delegate counter.
     */
    public OTelUpDownCounter(DoubleUpDownCounter doubleUpDownCounter) {
        this.doubleUpDownCounter = doubleUpDownCounter;
    }

    @Override
    public void add(double value) {
        doubleUpDownCounter.add(value);
    }

    @Override
    public void add(double value, Tags tags) {
        doubleUpDownCounter.add(value, OTelAttributesConverter.convert(tags));
    }
}
