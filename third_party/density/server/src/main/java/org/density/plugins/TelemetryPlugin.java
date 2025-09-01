/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.plugins;

import org.density.common.annotation.ExperimentalApi;
import org.density.telemetry.Telemetry;
import org.density.telemetry.TelemetrySettings;

import java.util.Optional;

/**
 * Plugin for extending telemetry related classes
 *
 * @density.experimental
 */
@ExperimentalApi
public interface TelemetryPlugin {

    Optional<Telemetry> getTelemetry(TelemetrySettings telemetrySettings);

    String getName();

}
