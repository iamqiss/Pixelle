/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.test.telemetry;

import org.density.plugins.Plugin;
import org.density.plugins.TelemetryPlugin;
import org.density.telemetry.Telemetry;
import org.density.telemetry.TelemetrySettings;

import java.util.Optional;

/**
 * Mock {@link TelemetryPlugin} implementation for testing.
 */
public class MockTelemetryPlugin extends Plugin implements TelemetryPlugin {
    private static final String MOCK_TRACER_NAME = "mock";

    /**
     * Base constructor.
     */
    public MockTelemetryPlugin() {

    }

    @Override
    public Optional<Telemetry> getTelemetry(TelemetrySettings settings) {
        return Optional.of(new MockTelemetry(settings));
    }

    @Override
    public String getName() {
        return MOCK_TRACER_NAME;
    }
}
