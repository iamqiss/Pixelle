/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.env;

import org.density.common.settings.Settings;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;
import org.density.core.transport.TransportResponse;

import java.io.IOException;
import java.util.Objects;

/**
 * Environment Settings Response for Extensibility
 *
 * @density.internal
 */
public class EnvironmentSettingsResponse extends TransportResponse {
    private final Settings environmentSettings;

    public EnvironmentSettingsResponse(Settings environmentSettings) {
        this.environmentSettings = environmentSettings;
    }

    public EnvironmentSettingsResponse(StreamInput in) throws IOException {
        this.environmentSettings = Settings.readSettingsFromStream(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Settings.writeSettingsToStream(this.environmentSettings, out);
    }

    public Settings getEnvironmentSettings() {
        return environmentSettings;
    }

    @Override
    public String toString() {
        return "EnvironmentSettingsResponse{environmentSettings=" + environmentSettings.toString() + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EnvironmentSettingsResponse that = (EnvironmentSettingsResponse) o;
        return Objects.equals(environmentSettings, that.environmentSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(environmentSettings);
    }
}
