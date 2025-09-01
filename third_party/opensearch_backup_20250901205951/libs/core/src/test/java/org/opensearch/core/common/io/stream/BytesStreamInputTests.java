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

/** test the BytesStreamInput using the same BaseStreamTests */
public class BytesStreamInputTests extends BaseStreamTests {
    @Override
    protected StreamInput getStreamInput(BytesReference bytesReference) throws IOException {
        BytesRef br = bytesReference.toBytesRef();
        return new BytesStreamInput(br.bytes, br.offset, br.length);
    }
}
