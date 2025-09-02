/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.cluster.coordination;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.density.Version;
import org.density.common.CheckedConsumer;
import org.density.common.io.stream.BytesStreamOutput;
import org.density.core.common.bytes.BytesReference;
import org.density.core.common.io.stream.InputStreamStreamInput;
import org.density.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.density.core.common.io.stream.NamedWriteableRegistry;
import org.density.core.common.io.stream.OutputStreamStreamOutput;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;
import org.density.core.compress.Compressor;
import org.density.core.compress.CompressorRegistry;
import org.density.transport.BytesTransportRequest;

import java.io.IOException;

/**
 * A helper class to utilize the compressed stream.
 *
 * @density.internal
 */
public final class CompressedStreamUtils {
    private static final Logger logger = LogManager.getLogger(CompressedStreamUtils.class);

    public static BytesReference createCompressedStream(Version version, CheckedConsumer<StreamOutput, IOException> outputConsumer)
        throws IOException {
        final BytesStreamOutput bStream = new BytesStreamOutput();
        try (StreamOutput stream = new OutputStreamStreamOutput(CompressorRegistry.defaultCompressor().threadLocalOutputStream(bStream))) {
            stream.setVersion(version);
            outputConsumer.accept(stream);
        }
        final BytesReference serializedByteRef = bStream.bytes();
        logger.trace("serialized writable object for node version [{}] with size [{}]", version, serializedByteRef.length());
        return serializedByteRef;
    }

    public static StreamInput decompressBytes(BytesTransportRequest request, NamedWriteableRegistry namedWriteableRegistry)
        throws IOException {
        final Compressor compressor = CompressorRegistry.compressor(request.bytes());
        final StreamInput in;
        if (compressor != null) {
            in = new InputStreamStreamInput(compressor.threadLocalInputStream(request.bytes().streamInput()));
        } else {
            in = request.bytes().streamInput();
        }
        in.setVersion(request.version());
        return new NamedWriteableAwareStreamInput(in, namedWriteableRegistry);
    }
}
