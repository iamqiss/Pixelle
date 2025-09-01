/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.telemetry.metrics;

import org.density.common.settings.ClusterSettings;
import org.density.common.settings.Setting;
import org.density.common.settings.Settings;
import org.density.common.util.FeatureFlags;
import org.density.telemetry.Telemetry;
import org.density.telemetry.TelemetrySettings;
import org.density.telemetry.metrics.noop.NoopCounter;
import org.density.telemetry.metrics.noop.NoopMetricsRegistry;
import org.density.telemetry.tracing.TracingTelemetry;
import org.density.test.DensityTestCase;
import org.junit.After;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MetricsRegistryFactoryTests extends DensityTestCase {

    private MetricsRegistryFactory metricsRegistryFactory;

    @After
    public void close() {
        metricsRegistryFactory.close();
    }

    public void testGetMeterRegistryWithUnavailableMetricsTelemetry() {
        Settings settings = Settings.builder().put(TelemetrySettings.TRACER_ENABLED_SETTING.getKey(), false).build();
        TelemetrySettings telemetrySettings = new TelemetrySettings(settings, new ClusterSettings(settings, getClusterSettings()));
        Telemetry mockTelemetry = mock(Telemetry.class);
        when(mockTelemetry.getTracingTelemetry()).thenReturn(mock(TracingTelemetry.class));
        metricsRegistryFactory = new MetricsRegistryFactory(telemetrySettings, Optional.empty());

        MetricsRegistry metricsRegistry = metricsRegistryFactory.getMetricsRegistry();

        assertTrue(metricsRegistry instanceof NoopMetricsRegistry);
        assertTrue(metricsRegistry.createCounter("test", "test", "test") == NoopCounter.INSTANCE);
        assertTrue(metricsRegistry.createUpDownCounter("test", "test", "test") == NoopCounter.INSTANCE);
    }

    public void testGetMetricsWithAvailableMetricsTelemetry() {
        Settings settings = Settings.builder().put(TelemetrySettings.TRACER_ENABLED_SETTING.getKey(), true).build();
        TelemetrySettings telemetrySettings = new TelemetrySettings(settings, new ClusterSettings(settings, getClusterSettings()));
        Telemetry mockTelemetry = mock(Telemetry.class);
        when(mockTelemetry.getMetricsTelemetry()).thenReturn(mock(MetricsTelemetry.class));
        metricsRegistryFactory = new MetricsRegistryFactory(telemetrySettings, Optional.of(mockTelemetry));

        MetricsRegistry metricsRegistry = metricsRegistryFactory.getMetricsRegistry();
        assertTrue(metricsRegistry instanceof DefaultMetricsRegistry);

    }

    public void testNullMetricsTelemetry() {
        Settings settings = Settings.builder().put(TelemetrySettings.METRICS_FEATURE_ENABLED_SETTING.getKey(), false).build();
        TelemetrySettings telemetrySettings = new TelemetrySettings(settings, new ClusterSettings(settings, getClusterSettings()));
        Telemetry mockTelemetry = mock(Telemetry.class);
        when(mockTelemetry.getMetricsTelemetry()).thenReturn(null);
        metricsRegistryFactory = new MetricsRegistryFactory(telemetrySettings, Optional.of(mockTelemetry));

        MetricsRegistry metricsRegistry = metricsRegistryFactory.getMetricsRegistry();
        assertTrue(metricsRegistry instanceof NoopMetricsRegistry);

    }

    private Set<Setting<?>> getClusterSettings() {
        Set<Setting<?>> allTracerSettings = new HashSet<>();
        ClusterSettings.FEATURE_FLAGGED_CLUSTER_SETTINGS.get(List.of(FeatureFlags.TELEMETRY)).stream().forEach((allTracerSettings::add));
        return allTracerSettings;
    }
}
