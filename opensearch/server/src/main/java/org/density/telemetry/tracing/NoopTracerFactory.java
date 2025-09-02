/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.telemetry.tracing;

import org.density.common.annotation.InternalApi;
import org.density.telemetry.tracing.noop.NoopTracer;

import java.util.Optional;

/**
 * No-op implementation of TracerFactory
 *
 * @density.internal
 */
@InternalApi
public class NoopTracerFactory extends TracerFactory {
    public NoopTracerFactory() {
        super(null, Optional.empty(), null);
    }

    @Override
    public Tracer getTracer() {
        return NoopTracer.INSTANCE;
    }

    @Override
    public void close() {

    }

}
