/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.telemetry.metrics.exporter;

import org.density.common.settings.Settings;
import org.density.telemetry.OTelTelemetrySettings;
import org.density.test.DensityTestCase;

import io.opentelemetry.exporter.logging.LoggingMetricExporter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.sdk.metrics.export.MetricExporter;

public class OTelMetricsExporterFactoryTests extends DensityTestCase {

    public void testMetricsExporterDefault() {
        Settings settings = Settings.builder().build();
        MetricExporter metricExporter = OTelMetricsExporterFactory.create(settings);
        assertTrue(metricExporter instanceof LoggingMetricExporter);
    }

    public void testMetricsExporterLogging() {
        Settings settings = Settings.builder()
            .put(
                OTelTelemetrySettings.OTEL_METRICS_EXPORTER_CLASS_SETTING.getKey(),
                "io.opentelemetry.exporter.logging.LoggingMetricExporter"
            )
            .build();
        MetricExporter metricExporter = OTelMetricsExporterFactory.create(settings);
        assertTrue(metricExporter instanceof LoggingMetricExporter);
    }

    public void testMetricExporterInvalid() {
        Settings settings = Settings.builder().put(OTelTelemetrySettings.OTEL_METRICS_EXPORTER_CLASS_SETTING.getKey(), "abc").build();
        assertThrows(IllegalArgumentException.class, () -> OTelMetricsExporterFactory.create(settings));
    }

    public void testMetricExporterNoCreateFactoryMethod() {
        Settings settings = Settings.builder()
            .put(
                OTelTelemetrySettings.OTEL_METRICS_EXPORTER_CLASS_SETTING.getKey(),
                "org.density.telemetry.metrics.exporter.DummyMetricExporter"
            )
            .build();
        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> OTelMetricsExporterFactory.create(settings));
        assertEquals(
            "MetricExporter instantiation failed for class [org.density.telemetry.metrics.exporter.DummyMetricExporter]",
            exception.getMessage()
        );
    }

    public void testMetricExporterNonMetricExporterClass() {
        Settings settings = Settings.builder()
            .put(OTelTelemetrySettings.OTEL_METRICS_EXPORTER_CLASS_SETTING.getKey(), "java.lang.String")
            .build();
        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> OTelMetricsExporterFactory.create(settings));
        assertEquals("MetricExporter instantiation failed for class [java.lang.String]", exception.getMessage());
        assertTrue(exception.getCause() instanceof NoSuchMethodError);

    }

    public void testMetricExporterGetDefaultMethod() {
        Settings settings = Settings.builder()
            .put(
                OTelTelemetrySettings.OTEL_METRICS_EXPORTER_CLASS_SETTING.getKey(),
                "io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter"
            )
            .build();

        assertTrue(OTelMetricsExporterFactory.create(settings) instanceof OtlpGrpcMetricExporter);
    }

}
