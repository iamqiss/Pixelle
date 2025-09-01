/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.telemetry.tracing;

import org.density.common.settings.ClusterSettings;
import org.density.common.settings.Setting;
import org.density.common.settings.Settings;
import org.density.common.util.FeatureFlags;
import org.density.common.util.concurrent.ThreadContext;
import org.density.telemetry.Telemetry;
import org.density.telemetry.TelemetrySettings;
import org.density.telemetry.tracing.attributes.Attributes;
import org.density.telemetry.tracing.noop.NoopSpan;
import org.density.telemetry.tracing.noop.NoopTracer;
import org.density.test.DensityTestCase;
import org.junit.After;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TracerFactoryTests extends DensityTestCase {

    private TracerFactory tracerFactory;

    @After
    public void close() {
        tracerFactory.close();
    }

    public void testGetTracerWithUnavailableTracingTelemetryReturnsNoopTracer() {
        Settings settings = Settings.builder().put(TelemetrySettings.TRACER_ENABLED_SETTING.getKey(), false).build();
        TelemetrySettings telemetrySettings = new TelemetrySettings(settings, new ClusterSettings(settings, getClusterSettings()));
        Telemetry mockTelemetry = mock(Telemetry.class);
        when(mockTelemetry.getTracingTelemetry()).thenReturn(mock(TracingTelemetry.class));
        tracerFactory = new TracerFactory(telemetrySettings, Optional.empty(), new ThreadContext(Settings.EMPTY));

        Tracer tracer = tracerFactory.getTracer();

        assertTrue(tracer instanceof NoopTracer);
        assertTrue(tracer.startSpan(SpanCreationContext.internal().name("foo").attributes(Attributes.EMPTY)) == NoopSpan.INSTANCE);
        assertTrue(tracer.startScopedSpan(SpanCreationContext.internal().name("foo").attributes(Attributes.EMPTY)) == ScopedSpan.NO_OP);
        assertTrue(tracer.startScopedSpan(SpanCreationContext.internal().name("foo").attributes(Attributes.EMPTY)) == ScopedSpan.NO_OP);
        assertTrue(
            tracer.withSpanInScope(
                tracer.startSpan(SpanCreationContext.internal().name("foo").attributes(Attributes.EMPTY))
            ) == SpanScope.NO_OP
        );
    }

    public void testGetTracerWithUnavailableTracingTelemetry() {
        Settings settings = Settings.builder().put(TelemetrySettings.TRACER_ENABLED_SETTING.getKey(), false).build();
        TelemetrySettings telemetrySettings = new TelemetrySettings(settings, new ClusterSettings(settings, getClusterSettings()));
        Telemetry mockTelemetry = mock(Telemetry.class);
        when(mockTelemetry.getTracingTelemetry()).thenReturn(mock(TracingTelemetry.class));
        tracerFactory = new TracerFactory(telemetrySettings, Optional.empty(), new ThreadContext(Settings.EMPTY));

        Tracer tracer = tracerFactory.getTracer();

        assertTrue(tracer instanceof NoopTracer);
        assertTrue(tracer.startScopedSpan(SpanCreationContext.internal().name("foo").attributes(Attributes.EMPTY)) == ScopedSpan.NO_OP);
    }

    public void testGetTracerWithAvailableTracingTelemetryReturnsWrappedTracer() {
        Settings settings = Settings.builder().put(TelemetrySettings.TRACER_ENABLED_SETTING.getKey(), true).build();
        TelemetrySettings telemetrySettings = new TelemetrySettings(settings, new ClusterSettings(settings, getClusterSettings()));
        Telemetry mockTelemetry = mock(Telemetry.class);
        when(mockTelemetry.getTracingTelemetry()).thenReturn(mock(TracingTelemetry.class));
        tracerFactory = new TracerFactory(telemetrySettings, Optional.of(mockTelemetry), new ThreadContext(Settings.EMPTY));

        Tracer tracer = tracerFactory.getTracer();
        assertTrue(tracer instanceof WrappedTracer);

    }

    public void testNullTracer() {
        Settings settings = Settings.builder().put(TelemetrySettings.TRACER_FEATURE_ENABLED_SETTING.getKey(), false).build();
        TelemetrySettings telemetrySettings = new TelemetrySettings(settings, new ClusterSettings(settings, getClusterSettings()));
        Telemetry mockTelemetry = mock(Telemetry.class);
        when(mockTelemetry.getTracingTelemetry()).thenReturn(null);
        tracerFactory = new TracerFactory(telemetrySettings, Optional.of(mockTelemetry), new ThreadContext(Settings.EMPTY));

        Tracer tracer = tracerFactory.getTracer();
        assertTrue(tracer instanceof NoopTracer);

    }

    private Set<Setting<?>> getClusterSettings() {
        Set<Setting<?>> allTracerSettings = new HashSet<>();
        ClusterSettings.FEATURE_FLAGGED_CLUSTER_SETTINGS.get(List.of(FeatureFlags.TELEMETRY)).stream().forEach((allTracerSettings::add));
        return allTracerSettings;
    }
}
