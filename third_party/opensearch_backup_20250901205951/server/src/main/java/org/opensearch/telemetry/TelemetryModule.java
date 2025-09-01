/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.telemetry;

import org.density.plugins.TelemetryPlugin;

import java.util.List;
import java.util.Optional;

/**
 * A module for loading classes for telemetry
 *
 * @density.internal
 */
public class TelemetryModule {

    private Telemetry telemetry;

    public TelemetryModule(List<TelemetryPlugin> telemetryPlugins, TelemetrySettings telemetrySettings) {

        for (TelemetryPlugin telemetryPlugin : telemetryPlugins) {
            Optional<Telemetry> telemetry = telemetryPlugin.getTelemetry(telemetrySettings);
            if (telemetry.isPresent()) {
                registerTelemetry(telemetry.get());
            }
        }
    }

    public Optional<Telemetry> getTelemetry() {
        return Optional.ofNullable(telemetry);
    }

    private void registerTelemetry(Telemetry factory) {
        if (telemetry == null) {
            telemetry = factory;
        } else {
            throw new IllegalArgumentException("Cannot register more than one telemetry");
        }
    }

}
