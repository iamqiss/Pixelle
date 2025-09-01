/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.http.reactor.netty4;

import org.density.core.common.bytes.BytesArray;
import org.density.core.common.bytes.BytesReference;
import org.density.http.HttpChunk;

import io.netty.buffer.ByteBuf;

class ReactorNetty4HttpChunk implements HttpChunk {
    private final BytesArray content;
    private final boolean last;

    ReactorNetty4HttpChunk(ByteBuf buf, boolean last) {
        // Since the chunks could be batched and processing could be delayed, we are copying the content here
        final byte[] content = new byte[buf.readableBytes()];
        buf.readBytes(content);
        this.content = new BytesArray(content);
        this.last = last;
    }

    @Override
    public BytesReference content() {
        return content;
    }

    @Override
    public void close() {}

    @Override
    public boolean isLast() {
        return last;
    }
}
