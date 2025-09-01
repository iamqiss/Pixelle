/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.core.compress;

import org.density.common.annotation.PublicApi;
import org.density.core.common.bytes.BytesReference;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * {@link Compressor} no compressor implementation.
 *
 * @density.api - registered name requires BWC support
 * @density.experimental - class methods might change
 */
public class NoneCompressor implements Compressor {
    /**
     * The name to register the compressor by
     *
     * @density.api - requires BWC support
     */
    @PublicApi(since = "2.10.0")
    public static final String NAME = "NONE";

    @Override
    public boolean isCompressed(BytesReference bytes) {
        return false;
    }

    @Override
    public int headerLength() {
        return 0;
    }

    @Override
    public InputStream threadLocalInputStream(InputStream in) throws IOException {
        return in;
    }

    @Override
    public OutputStream threadLocalOutputStream(OutputStream out) throws IOException {
        return out;
    }

    @Override
    public BytesReference uncompress(BytesReference bytesReference) throws IOException {
        return bytesReference;
    }

    @Override
    public BytesReference compress(BytesReference bytesReference) throws IOException {
        return bytesReference;
    }

}
