/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.compress;

import org.density.core.compress.Compressor;
import org.density.test.core.compress.AbstractCompressorTestCase;

/**
 * Test streaming compression
 */
public class ZstdCompressTests extends AbstractCompressorTestCase {

    private final Compressor compressor = new ZstdCompressor();

    @Override
    protected Compressor compressor() {
        return compressor;
    }
}
