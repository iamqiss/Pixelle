/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.common.xcontent.support;

import org.density.common.Nullable;
import org.density.common.lease.Releasable;
import org.density.core.common.bytes.BytesArray;
import org.density.core.common.bytes.BytesReference;
import org.density.core.common.bytes.CompositeBytesReference;
import org.density.core.xcontent.XContentBuilder;
import org.density.http.HttpChunk;

/**
 * Wraps the instance of the {@link XContentBuilder} into {@link HttpChunk}
 */
public final class XContentHttpChunk implements HttpChunk {
    private static final byte[] CHUNK_SEPARATOR = new byte[] { '\r', '\n' };
    private final BytesReference content;

    /**
     * Creates a new {@link HttpChunk} from {@link XContentBuilder}
     * @param builder {@link XContentBuilder} instance
     * @return new {@link HttpChunk} instance, if passed {@link XContentBuilder} us {@code null}, a last empty {@link HttpChunk} will be returned
     */
    public static HttpChunk from(@Nullable final XContentBuilder builder) {
        return new XContentHttpChunk(builder);
    }

    /**
     * Creates a new last empty {@link HttpChunk}
     * @return last empty {@link HttpChunk} instance
     */
    public static HttpChunk last() {
        return new XContentHttpChunk(null);
    }

    private XContentHttpChunk(@Nullable final XContentBuilder builder) {
        if (builder == null /* no content */) {
            content = BytesArray.EMPTY;
        } else {
            // Always finalize the output chunk with '\r\n' sequence
            content = CompositeBytesReference.of(BytesReference.bytes(builder), new BytesArray(CHUNK_SEPARATOR));
        }
    }

    @Override
    public boolean isLast() {
        return content == BytesArray.EMPTY;
    }

    @Override
    public BytesReference content() {
        return content;
    }

    @Override
    public void close() {
        if (content instanceof Releasable) {
            ((Releasable) content).close();
        }
    }
}
