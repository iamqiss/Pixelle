/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.cluster;

import org.density.cluster.service.ClusterService;
import org.density.common.settings.Settings;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;
import org.density.core.transport.TransportResponse;

import java.io.IOException;
import java.util.Objects;

/**
 * PluginSettings Response for Extensibility
 *
 * @density.internal
 */
public class ClusterSettingsResponse extends TransportResponse {
    private final Settings clusterSettings;

    public ClusterSettingsResponse(ClusterService clusterService) {
        this.clusterSettings = clusterService.getSettings();
    }

    public ClusterSettingsResponse(StreamInput in) throws IOException {
        super(in);
        this.clusterSettings = Settings.readSettingsFromStream(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Settings.writeSettingsToStream(clusterSettings, out);
    }

    @Override
    public String toString() {
        return "ClusterSettingsResponse{" + "clusterSettings=" + clusterSettings + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClusterSettingsResponse that = (ClusterSettingsResponse) o;
        return Objects.equals(clusterSettings, that.clusterSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clusterSettings);
    }

}
