/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.core.common.io.stream;

import org.apache.lucene.util.BytesRef;
import org.density.core.common.bytes.BytesReference;

import java.io.IOException;
import java.nio.ByteBuffer;

/** test the ByteBufferStreamInput using the same BaseStreamTests */
public class ByteBufferStreamInputTests extends BaseStreamTests {
    @Override
    protected StreamInput getStreamInput(BytesReference bytesReference) throws IOException {
        BytesRef br = bytesReference.toBytesRef();
        ByteBuffer bb = ByteBuffer.wrap(br.bytes, br.offset, br.length);
        return new ByteBufferStreamInput(bb);
    }
}
