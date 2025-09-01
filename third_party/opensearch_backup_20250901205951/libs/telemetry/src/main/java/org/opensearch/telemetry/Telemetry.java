/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.telemetry;

import org.density.common.annotation.ExperimentalApi;
import org.density.telemetry.metrics.MetricsTelemetry;
import org.density.telemetry.tracing.TracingTelemetry;

/**
 * Interface defining telemetry
 *
 * @density.experimental
 */
@ExperimentalApi
public interface Telemetry {

    /**
     * Provides tracing telemetry
     * @return tracing telemetry instance
     */
    TracingTelemetry getTracingTelemetry();

    /**
     * Provides metrics telemetry
     * @return metrics telemetry instance
     */
    MetricsTelemetry getMetricsTelemetry();

}
