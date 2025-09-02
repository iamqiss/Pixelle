/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.indices;

import org.density.DensityException;
import org.density.common.cache.serializer.Serializer;
import org.density.common.io.stream.BytesStreamOutput;
import org.density.core.common.bytes.BytesReference;
import org.density.core.common.io.stream.BytesStreamInput;

import java.io.IOException;
import java.util.Arrays;

/**
 * This class serializes the IndicesRequestCache.Key using its writeTo method.
 */
public class IRCKeyWriteableSerializer implements Serializer<IndicesRequestCache.Key, byte[]> {

    public IRCKeyWriteableSerializer() {}

    @Override
    public byte[] serialize(IndicesRequestCache.Key object) {
        if (object == null) {
            return null;
        }
        try {
            BytesStreamOutput os = new BytesStreamOutput();
            object.writeTo(os);
            return BytesReference.toBytes(os.bytes());
        } catch (IOException e) {
            throw new DensityException("Unable to serialize IndicesRequestCache.Key", e);
        }
    }

    @Override
    public IndicesRequestCache.Key deserialize(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        try {
            BytesStreamInput is = new BytesStreamInput(bytes, 0, bytes.length);
            return new IndicesRequestCache.Key(is);
        } catch (IOException e) {
            throw new DensityException("Unable to deserialize byte[] to IndicesRequestCache.Key", e);
        }
    }

    @Override
    public boolean equals(IndicesRequestCache.Key object, byte[] bytes) {
        // Deserialization is much slower than serialization for keys of order 1 KB,
        // while time to serialize is fairly constant (per byte)
        if (bytes.length < 5000) {
            return Arrays.equals(serialize(object), bytes);
        } else {
            return object.equals(deserialize(bytes));
        }
    }
}
