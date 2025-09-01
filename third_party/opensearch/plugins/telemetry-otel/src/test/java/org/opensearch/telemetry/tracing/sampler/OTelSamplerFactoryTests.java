/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.density.telemetry.tracing.sampler;

import org.density.common.settings.ClusterSettings;
import org.density.common.settings.Settings;
import org.density.telemetry.OTelTelemetrySettings;
import org.density.telemetry.TelemetrySettings;
import org.density.test.DensityTestCase;

import java.util.Set;

import io.opentelemetry.sdk.trace.samplers.Sampler;

import static org.density.telemetry.TelemetrySettings.TRACER_ENABLED_SETTING;
import static org.density.telemetry.TelemetrySettings.TRACER_SAMPLER_PROBABILITY;

public class OTelSamplerFactoryTests extends DensityTestCase {

    public void testDefaultCreate() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, Set.of(TRACER_SAMPLER_PROBABILITY, TRACER_ENABLED_SETTING));
        TelemetrySettings telemetrySettings = new TelemetrySettings(Settings.EMPTY, clusterSettings);
        Sampler sampler = OTelSamplerFactory.create(telemetrySettings, Settings.EMPTY);
        assertEquals(sampler.getClass(), ProbabilisticTransportActionSampler.class);
    }

    public void testCreateWithSingleSampler() {
        Settings settings = Settings.builder()
            .put(OTelTelemetrySettings.OTEL_TRACER_SPAN_SAMPLER_CLASS_SETTINGS.getKey(), ProbabilisticSampler.class.getName())
            .build();

        ClusterSettings clusterSettings = new ClusterSettings(settings, Set.of(TRACER_SAMPLER_PROBABILITY, TRACER_ENABLED_SETTING));
        TelemetrySettings telemetrySettings = new TelemetrySettings(settings, clusterSettings);
        Sampler sampler = OTelSamplerFactory.create(telemetrySettings, settings);
        assertTrue(sampler instanceof ProbabilisticSampler);
    }
}
