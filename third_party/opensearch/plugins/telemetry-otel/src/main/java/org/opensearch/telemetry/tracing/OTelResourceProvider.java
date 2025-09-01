/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.telemetry.tracing;

import org.density.common.settings.Settings;
import org.density.telemetry.TelemetrySettings;
import org.density.telemetry.metrics.exporter.OTelMetricsExporterFactory;
import org.density.telemetry.tracing.exporter.OTelSpanExporterFactory;
import org.density.telemetry.tracing.sampler.OTelSamplerFactory;
import org.density.telemetry.tracing.sampler.RequestSampler;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.TimeUnit;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.InstrumentSelector;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.View;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.metrics.internal.view.Base2ExponentialHistogramAggregation;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.semconv.ServiceAttributes;

import static org.density.telemetry.OTelTelemetrySettings.TRACER_EXPORTER_BATCH_SIZE_SETTING;
import static org.density.telemetry.OTelTelemetrySettings.TRACER_EXPORTER_DELAY_SETTING;
import static org.density.telemetry.OTelTelemetrySettings.TRACER_EXPORTER_MAX_QUEUE_SIZE_SETTING;

/**
 * This class encapsulates all OpenTelemetry related resources
 */
public final class OTelResourceProvider {

    private OTelResourceProvider() {}

    /**
     * Creates OpenTelemetry instance with default configuration
     * @param telemetrySettings telemetry settings
     * @param settings cluster settings
     * @return OpenTelemetrySdk instance
     */
    @SuppressWarnings("removal")
    public static OpenTelemetrySdk get(TelemetrySettings telemetrySettings, Settings settings) {
        return AccessController.doPrivileged(
            (PrivilegedAction<OpenTelemetrySdk>) () -> get(
                settings,
                OTelSpanExporterFactory.create(settings),
                ContextPropagators.create(W3CTraceContextPropagator.getInstance()),
                Sampler.parentBased(new RequestSampler(OTelSamplerFactory.create(telemetrySettings, settings)))
            )
        );
    }

    /**
     * Creates OpenTelemetry instance with provided configuration
     * @param settings cluster settings
     * @param spanExporter span exporter instance
     * @param contextPropagators context propagator instance
     * @param sampler sampler instance
     * @return OpenTelemetrySdk instance
     */
    public static OpenTelemetrySdk get(
        Settings settings,
        SpanExporter spanExporter,
        ContextPropagators contextPropagators,
        Sampler sampler
    ) {
        Resource resource = Resource.create(Attributes.of(ServiceAttributes.SERVICE_NAME, "Density"));
        SdkTracerProvider sdkTracerProvider = createSdkTracerProvider(settings, spanExporter, sampler, resource);
        SdkMeterProvider sdkMeterProvider = createSdkMetricProvider(settings, resource);
        return OpenTelemetrySdk.builder()
            .setTracerProvider(sdkTracerProvider)
            .setMeterProvider(sdkMeterProvider)
            .setPropagators(contextPropagators)
            .buildAndRegisterGlobal();
    }

    private static SdkMeterProvider createSdkMetricProvider(Settings settings, Resource resource) {
        return SdkMeterProvider.builder()
            .setResource(resource)
            .registerMetricReader(
                PeriodicMetricReader.builder(OTelMetricsExporterFactory.create(settings))
                    .setInterval(TelemetrySettings.METRICS_PUBLISH_INTERVAL_SETTING.get(settings).getSeconds(), TimeUnit.SECONDS)
                    .build()
            )
            .registerView(
                InstrumentSelector.builder().setType(InstrumentType.HISTOGRAM).build(),
                View.builder().setAggregation(Base2ExponentialHistogramAggregation.getDefault()).build()
            )
            .build();
    }

    private static SdkTracerProvider createSdkTracerProvider(
        Settings settings,
        SpanExporter spanExporter,
        Sampler sampler,
        Resource resource
    ) {
        return SdkTracerProvider.builder()
            .addSpanProcessor(spanProcessor(settings, spanExporter))
            .setResource(resource)
            .setSampler(sampler)
            .build();
    }

    private static BatchSpanProcessor spanProcessor(Settings settings, SpanExporter spanExporter) {
        return BatchSpanProcessor.builder(spanExporter)
            .setScheduleDelay(TRACER_EXPORTER_DELAY_SETTING.get(settings).getSeconds(), TimeUnit.SECONDS)
            .setMaxExportBatchSize(TRACER_EXPORTER_BATCH_SIZE_SETTING.get(settings))
            .setMaxQueueSize(TRACER_EXPORTER_MAX_QUEUE_SIZE_SETTING.get(settings))
            .build();
    }

}
