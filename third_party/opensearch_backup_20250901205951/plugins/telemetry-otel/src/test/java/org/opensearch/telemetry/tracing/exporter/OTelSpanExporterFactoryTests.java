/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.telemetry.tracing.exporter;

import org.density.common.settings.Settings;
import org.density.telemetry.OTelTelemetrySettings;
import org.density.test.DensityTestCase;

import io.opentelemetry.exporter.logging.LoggingSpanExporter;
import io.opentelemetry.sdk.trace.export.SpanExporter;

public class OTelSpanExporterFactoryTests extends DensityTestCase {

    public void testSpanExporterDefault() {
        Settings settings = Settings.builder().build();
        SpanExporter spanExporter = OTelSpanExporterFactory.create(settings);
        assertTrue(spanExporter instanceof LoggingSpanExporter);
    }

    public void testSpanExporterLogging() {
        Settings settings = Settings.builder()
            .put(
                OTelTelemetrySettings.OTEL_TRACER_SPAN_EXPORTER_CLASS_SETTING.getKey(),
                "io.opentelemetry.exporter.logging.LoggingSpanExporter"
            )
            .build();
        SpanExporter spanExporter = OTelSpanExporterFactory.create(settings);
        assertTrue(spanExporter instanceof LoggingSpanExporter);
    }

    public void testSpanExporterInvalid() {
        Settings settings = Settings.builder().put(OTelTelemetrySettings.OTEL_TRACER_SPAN_EXPORTER_CLASS_SETTING.getKey(), "abc").build();
        assertThrows(IllegalArgumentException.class, () -> OTelSpanExporterFactory.create(settings));
    }

    public void testSpanExporterNoCreateFactoryMethod() {
        Settings settings = Settings.builder()
            .put(
                OTelTelemetrySettings.OTEL_TRACER_SPAN_EXPORTER_CLASS_SETTING.getKey(),
                "org.density.telemetry.tracing.exporter.DummySpanExporter"
            )
            .build();
        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> OTelSpanExporterFactory.create(settings));
        assertEquals(
            "SpanExporter instantiation failed for class [org.density.telemetry.tracing.exporter.DummySpanExporter]",
            exception.getMessage()
        );
    }

    public void testSpanExporterNonSpanExporterClass() {
        Settings settings = Settings.builder()
            .put(OTelTelemetrySettings.OTEL_TRACER_SPAN_EXPORTER_CLASS_SETTING.getKey(), "java.lang.String")
            .build();
        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> OTelSpanExporterFactory.create(settings));
        assertEquals("SpanExporter instantiation failed for class [java.lang.String]", exception.getMessage());
        assertTrue(exception.getCause() instanceof NoSuchMethodError);

    }

    public void testSpanExporterGetDefaultMethod() {
        Settings settings = Settings.builder()
            .put(
                OTelTelemetrySettings.OTEL_TRACER_SPAN_EXPORTER_CLASS_SETTING.getKey(),
                "org.density.telemetry.tracing.exporter.DummySpanExporterWithGetDefault"
            )
            .build();

        assertTrue(OTelSpanExporterFactory.create(settings) instanceof DummySpanExporterWithGetDefault);
    }

}
