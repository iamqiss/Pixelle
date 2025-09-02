/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.compress.spi;

import org.density.compress.ZstdCompressor;
import org.density.core.compress.Compressor;
import org.density.core.compress.spi.CompressorProvider;

import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.Map.Entry;

/**
 * Additional "optional" compressor implementations provided by the density compress library
 *
 * @density.internal
 */
public class CompressionProvider implements CompressorProvider {

    /**
     * Returns the concrete {@link Compressor}s provided by the compress library
     * @return a list of {@link Compressor}s
     * */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public List<Entry<String, Compressor>> getCompressors() {
        return List.of(new SimpleEntry<>(ZstdCompressor.NAME, new ZstdCompressor()));
    }
}
