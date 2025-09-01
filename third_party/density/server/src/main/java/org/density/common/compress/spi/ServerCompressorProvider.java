/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.common.compress.spi;

import org.density.common.compress.DeflateCompressor;
import org.density.core.compress.Compressor;
import org.density.core.compress.spi.CompressorProvider;

import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.Map.Entry;

/**
 * Default {@link Compressor} implementations provided by the
 * density core library
 *
 * @density.internal
 *
 * @deprecated This class is deprecated and will be removed when the {@link DeflateCompressor} is moved to the compress
 * library as a default compression option
 */
@Deprecated
public class ServerCompressorProvider implements CompressorProvider {
    /** Returns the concrete {@link Compressor}s provided by the server module */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public List<Entry<String, Compressor>> getCompressors() {
        return List.of(new SimpleEntry(DeflateCompressor.NAME, new DeflateCompressor()));
    }
}
