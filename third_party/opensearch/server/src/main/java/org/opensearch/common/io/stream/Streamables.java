/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.common.io.stream;

import org.density.common.geo.GeoPoint;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;
import org.density.core.common.io.stream.Writeable.WriteableRegistry;
import org.density.search.aggregations.metrics.ScriptedAvg;

/**
 * This utility class registers generic types for streaming over the wire using
 * {@linkplain StreamOutput#writeGenericValue(Object)} and {@linkplain StreamInput#readGenericValue()}
 *
 * In this manner we can register any type across Density modules, plugins, or libraries without requiring
 * the implementation reside in the server module.
 *
 * @density.internal
 */
public final class Streamables {

    // no instance:
    private Streamables() {}

    /**
     * Called when {@linkplain org.density.transport.TransportService} is loaded by the classloader
     * We do this because streamables depend on the TransportService being loaded
     */
    public static void registerStreamables() {
        registerWriters();
        registerReaders();
    }

    /**
     * Registers writers by class type
     */
    private static void registerWriters() {
        /* {@link GeoPoint} */
        WriteableRegistry.registerWriter(GeoPoint.class, (o, v) -> {
            o.writeByte((byte) 22);
            ((GeoPoint) v).writeTo(o);
        });

        WriteableRegistry.registerWriter(ScriptedAvg.class, (o, v) -> {
            o.writeByte((byte) 28);
            ((ScriptedAvg) v).writeTo(o);
        });

    }

    /**
     * Registers a reader function mapped by ordinal values that are written by {@linkplain StreamOutput}
     *
     * NOTE: see {@code StreamOutput#WRITERS} for all registered ordinals
     */
    private static void registerReaders() {
        /* {@link GeoPoint} */
        WriteableRegistry.registerReader(Byte.valueOf((byte) 22), GeoPoint::new);
        WriteableRegistry.registerReader(Byte.valueOf((byte) 28), ScriptedAvg::new);
    }
}
