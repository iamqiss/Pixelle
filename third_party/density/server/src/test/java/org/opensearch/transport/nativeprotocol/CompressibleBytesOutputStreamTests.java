/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright Density Contributors. See
 * GitHub history for details.
 */

package org.density.transport.nativeprotocol;

import org.density.common.io.stream.BytesStreamOutput;
import org.density.core.common.bytes.BytesReference;
import org.density.core.common.io.stream.BytesStream;
import org.density.core.common.io.stream.InputStreamStreamInput;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.compress.CompressorRegistry;
import org.density.test.DensityTestCase;

import java.io.EOFException;
import java.io.IOException;

public class CompressibleBytesOutputStreamTests extends DensityTestCase {

    public void testStreamWithoutCompression() throws IOException {
        BytesStream bStream = new ZeroOutOnCloseStream();
        CompressibleBytesOutputStream stream = new CompressibleBytesOutputStream(bStream, false);

        byte[] expectedBytes = randomBytes(randomInt(30));
        stream.write(expectedBytes);

        BytesReference bytesRef = stream.materializeBytes();
        // Closing compression stream does not close underlying stream
        stream.close();

        assertFalse(CompressorRegistry.defaultCompressor().isCompressed(bytesRef));

        StreamInput streamInput = bytesRef.streamInput();
        byte[] actualBytes = new byte[expectedBytes.length];
        streamInput.readBytes(actualBytes, 0, expectedBytes.length);

        assertEquals(-1, streamInput.read());
        assertArrayEquals(expectedBytes, actualBytes);

        bStream.close();

        // The bytes should be zeroed out on close
        for (byte b : bytesRef.toBytesRef().bytes) {
            assertEquals((byte) 0, b);
        }
    }

    public void testStreamWithCompression() throws IOException {
        BytesStream bStream = new ZeroOutOnCloseStream();
        CompressibleBytesOutputStream stream = new CompressibleBytesOutputStream(bStream, true);

        byte[] expectedBytes = randomBytes(randomInt(30));
        stream.write(expectedBytes);

        BytesReference bytesRef = stream.materializeBytes();
        stream.close();

        assertTrue(CompressorRegistry.defaultCompressor().isCompressed(bytesRef));

        StreamInput streamInput = new InputStreamStreamInput(
            CompressorRegistry.defaultCompressor().threadLocalInputStream(bytesRef.streamInput())
        );
        byte[] actualBytes = new byte[expectedBytes.length];
        streamInput.readBytes(actualBytes, 0, expectedBytes.length);

        assertEquals(-1, streamInput.read());
        assertArrayEquals(expectedBytes, actualBytes);

        bStream.close();

        // The bytes should be zeroed out on close
        for (byte b : bytesRef.toBytesRef().bytes) {
            assertEquals((byte) 0, b);
        }
    }

    public void testCompressionWithCallingMaterializeFails() throws IOException {
        BytesStream bStream = new ZeroOutOnCloseStream();
        CompressibleBytesOutputStream stream = new CompressibleBytesOutputStream(bStream, true);

        byte[] expectedBytes = randomBytes(between(1, 30));
        stream.write(expectedBytes);

        StreamInput streamInput = new InputStreamStreamInput(
            CompressorRegistry.defaultCompressor().threadLocalInputStream(bStream.bytes().streamInput())
        );
        byte[] actualBytes = new byte[expectedBytes.length];
        EOFException e = expectThrows(EOFException.class, () -> streamInput.readBytes(actualBytes, 0, expectedBytes.length));
        assertEquals("Unexpected end of ZLIB input stream", e.getMessage());

        stream.close();
    }

    private static byte[] randomBytes(int length) {
        byte[] bytes = new byte[length];
        for (int i = 0; i < bytes.length; ++i) {
            bytes[i] = randomByte();
        }
        return bytes;
    }

    private static class ZeroOutOnCloseStream extends BytesStreamOutput {

        @Override
        public void close() {
            if (bytes != null) {
                int size = (int) bytes.size();
                bytes.set(0, new byte[size], 0, size);
            }
        }
    }
}
