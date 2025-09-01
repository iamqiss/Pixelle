/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.core.compress.spi;

import org.density.common.annotation.ExperimentalApi;
import org.density.common.annotation.PublicApi;
import org.density.core.compress.Compressor;

import java.util.List;
import java.util.Map;

/**
 * Service Provider Interface for plugins, modules, extensions providing custom
 * compression algorithms
 * <p>
 * see {@link Compressor} for implementing methods
 * and {@link org.density.core.compress.CompressorRegistry} for the registration of custom
 * Compressors
 *
 * @density.experimental
 * @density.api
 */
@ExperimentalApi
@PublicApi(since = "2.10.0")
public interface CompressorProvider {
    /** Extensions that implement their own concrete {@link Compressor}s provide them through this interface method*/
    List<Map.Entry<String, Compressor>> getCompressors();
}
