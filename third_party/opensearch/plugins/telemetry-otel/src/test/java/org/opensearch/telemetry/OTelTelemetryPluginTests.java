/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.telemetry;

import org.density.common.settings.ClusterSettings;
import org.density.common.settings.Setting;
import org.density.common.settings.Settings;
import org.density.common.util.FeatureFlags;
import org.density.telemetry.metrics.MetricsTelemetry;
import org.density.telemetry.metrics.OTelMetricsTelemetry;
import org.density.telemetry.tracing.OTelTracingTelemetry;
import org.density.telemetry.tracing.TracingTelemetry;
import org.density.test.DensityTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.density.telemetry.OTelTelemetryPlugin.OTEL_TRACER_NAME;
import static org.density.telemetry.OTelTelemetrySettings.OTEL_METRICS_EXPORTER_CLASS_SETTING;
import static org.density.telemetry.OTelTelemetrySettings.OTEL_TRACER_SPAN_EXPORTER_CLASS_SETTING;
import static org.density.telemetry.OTelTelemetrySettings.OTEL_TRACER_SPAN_SAMPLER_CLASS_SETTINGS;
import static org.density.telemetry.OTelTelemetrySettings.TRACER_EXPORTER_BATCH_SIZE_SETTING;
import static org.density.telemetry.OTelTelemetrySettings.TRACER_EXPORTER_DELAY_SETTING;
import static org.density.telemetry.OTelTelemetrySettings.TRACER_EXPORTER_MAX_QUEUE_SIZE_SETTING;
import static org.density.telemetry.OTelTelemetrySettings.TRACER_SAMPLER_ACTION_PROBABILITY;
import static org.density.telemetry.TelemetrySettings.TRACER_ENABLED_SETTING;
import static org.density.telemetry.TelemetrySettings.TRACER_SAMPLER_PROBABILITY;

public class OTelTelemetryPluginTests extends DensityTestCase {

    private OTelTelemetryPlugin oTelTelemetryPlugin;
    private Optional<Telemetry> telemetry;
    private TracingTelemetry tracingTelemetry;

    private MetricsTelemetry metricsTelemetry;

    @Before
    public void setup() {
        // TRACER_EXPORTER_DELAY_SETTING should always be less than 10 seconds because
        // io.opentelemetry.sdk.OpenTelemetrySdk.close waits only for 10 seconds for shutdown to complete.
        Settings settings = Settings.builder().put(TRACER_EXPORTER_DELAY_SETTING.getKey(), "1s").build();
        oTelTelemetryPlugin = new OTelTelemetryPlugin(settings);
        telemetry = oTelTelemetryPlugin.getTelemetry(
            new TelemetrySettings(Settings.EMPTY, new ClusterSettings(settings, Set.of(TRACER_ENABLED_SETTING, TRACER_SAMPLER_PROBABILITY)))
        );
        tracingTelemetry = telemetry.get().getTracingTelemetry();
        metricsTelemetry = telemetry.get().getMetricsTelemetry();
    }

    public void testGetTelemetry() {
        Set<Setting<?>> allTracerSettings = new HashSet<>();
        ClusterSettings.FEATURE_FLAGGED_CLUSTER_SETTINGS.get(List.of(FeatureFlags.TELEMETRY)).stream().forEach((allTracerSettings::add));
        assertEquals(OTEL_TRACER_NAME, oTelTelemetryPlugin.getName());
        assertTrue(tracingTelemetry instanceof OTelTracingTelemetry);
        assertTrue(metricsTelemetry instanceof OTelMetricsTelemetry);
        assertEquals(
            Arrays.asList(
                TRACER_EXPORTER_BATCH_SIZE_SETTING,
                TRACER_EXPORTER_DELAY_SETTING,
                TRACER_EXPORTER_MAX_QUEUE_SIZE_SETTING,
                OTEL_TRACER_SPAN_EXPORTER_CLASS_SETTING,
                OTEL_TRACER_SPAN_SAMPLER_CLASS_SETTINGS,
                OTEL_METRICS_EXPORTER_CLASS_SETTING,
                TRACER_SAMPLER_ACTION_PROBABILITY
            ),
            oTelTelemetryPlugin.getSettings()
        );

    }

    @After
    public void cleanup() throws IOException {
        tracingTelemetry.close();
        metricsTelemetry.close();
    }
}
