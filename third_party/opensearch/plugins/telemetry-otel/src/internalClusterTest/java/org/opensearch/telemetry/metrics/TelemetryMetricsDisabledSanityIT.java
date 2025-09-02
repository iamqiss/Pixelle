/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.telemetry.metrics;

import org.density.common.settings.Settings;
import org.density.common.unit.TimeValue;
import org.density.plugins.Plugin;
import org.density.telemetry.IntegrationTestOTelTelemetryPlugin;
import org.density.telemetry.OTelTelemetrySettings;
import org.density.telemetry.TelemetrySettings;
import org.density.telemetry.metrics.noop.NoopCounter;
import org.density.telemetry.metrics.noop.NoopHistogram;
import org.density.telemetry.metrics.noop.NoopMetricsRegistry;
import org.density.test.DensityIntegTestCase;

import java.util.Arrays;
import java.util.Collection;

@DensityIntegTestCase.ClusterScope(scope = DensityIntegTestCase.Scope.TEST, minNumDataNodes = 1)
public class TelemetryMetricsDisabledSanityIT extends DensityIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(TelemetrySettings.METRICS_FEATURE_ENABLED_SETTING.getKey(), false)
            .put(
                OTelTelemetrySettings.OTEL_METRICS_EXPORTER_CLASS_SETTING.getKey(),
                "org.density.telemetry.metrics.InMemorySingletonMetricsExporter"
            )
            .put(TelemetrySettings.METRICS_PUBLISH_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(1))
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(IntegrationTestOTelTelemetryPlugin.class);
    }

    @Override
    protected boolean addMockTelemetryPlugin() {
        return false;
    }

    public void testSanityChecksWhenMetricsDisabled() throws Exception {
        MetricsRegistry metricsRegistry = internalCluster().getInstance(MetricsRegistry.class);

        Counter counter = metricsRegistry.createCounter("test-counter", "test", "1");
        counter.add(1.0);

        Histogram histogram = metricsRegistry.createHistogram("test-histogram", "test", "1");

        Thread.sleep(2000);

        assertTrue(metricsRegistry instanceof NoopMetricsRegistry);
        assertTrue(counter instanceof NoopCounter);
        assertTrue(histogram instanceof NoopHistogram);
    }

}
