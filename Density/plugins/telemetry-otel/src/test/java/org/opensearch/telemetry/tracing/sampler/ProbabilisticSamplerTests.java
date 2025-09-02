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
import org.density.telemetry.TelemetrySettings;
import org.density.test.DensityTestCase;

import java.util.Set;

import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.trace.samplers.Sampler;

import static org.density.telemetry.OTelTelemetrySettings.TRACER_EXPORTER_DELAY_SETTING;
import static org.density.telemetry.TelemetrySettings.TRACER_ENABLED_SETTING;
import static org.density.telemetry.TelemetrySettings.TRACER_SAMPLER_PROBABILITY;
import static org.mockito.Mockito.mock;

public class ProbabilisticSamplerTests extends DensityTestCase {

    // When ProbabilisticSampler is created with OTelTelemetrySettings as null
    public void testProbabilisticSamplerWithNullSettings() {
        // Verify that the constructor throws IllegalArgumentException when given null settings
        assertThrows(NullPointerException.class, () -> { ProbabilisticSampler.create(null, null, null); });
    }

    public void testDefaultGetSampler() {
        Settings settings = Settings.builder().put(TRACER_EXPORTER_DELAY_SETTING.getKey(), "1s").build();
        TelemetrySettings telemetrySettings = new TelemetrySettings(
            Settings.EMPTY,
            new ClusterSettings(settings, Set.of(TRACER_SAMPLER_PROBABILITY, TRACER_ENABLED_SETTING))
        );

        // Probabilistic Sampler
        Sampler probabilisticSampler = ProbabilisticSampler.create(telemetrySettings, Settings.EMPTY, null);

        assertEquals(0.01, ((ProbabilisticSampler) probabilisticSampler).getSamplingRatio(), 0.0d);
    }

    public void testGetSamplerWithUpdatedSamplingRatio() {
        Settings settings = Settings.builder().put(TRACER_EXPORTER_DELAY_SETTING.getKey(), "1s").build();
        TelemetrySettings telemetrySettings = new TelemetrySettings(
            Settings.EMPTY,
            new ClusterSettings(settings, Set.of(TRACER_SAMPLER_PROBABILITY, TRACER_ENABLED_SETTING))
        );

        // Probabilistic Sampler
        Sampler probabilisticSampler = ProbabilisticSampler.create(telemetrySettings, Settings.EMPTY, null);

        assertEquals(0.01d, ((ProbabilisticSampler) probabilisticSampler).getSamplingRatio(), 0.0d);

        telemetrySettings.setSamplingProbability(0.02);

        // Need to call shouldSample() to update the value of samplingRatio
        probabilisticSampler.shouldSample(mock(Context.class), "00000000000000000000000000000000", "", SpanKind.INTERNAL, null, null);

        // Need to call getSampler() to update the value of tracerHeadSamplerSamplingRatio
        assertEquals(0.02, ((ProbabilisticSampler) probabilisticSampler).getSamplingRatio(), 0.0d);
    }
}
