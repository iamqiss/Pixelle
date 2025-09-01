/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.telemetry.metrics;

import org.density.common.annotation.InternalApi;
import org.density.telemetry.metrics.noop.NoopMetricsRegistry;

import java.util.Optional;

/**
 * No-op implementation of {@link MetricsRegistryFactory}
 *
 * @density.internal
 */
@InternalApi
public class NoopMetricsRegistryFactory extends MetricsRegistryFactory {
    public NoopMetricsRegistryFactory() {
        super(null, Optional.empty());
    }

    @Override
    public MetricsRegistry getMetricsRegistry() {
        return NoopMetricsRegistry.INSTANCE;
    }

    @Override
    public void close() {

    }
}
