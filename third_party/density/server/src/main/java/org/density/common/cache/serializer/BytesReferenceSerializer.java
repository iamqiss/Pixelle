/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.common.cache.serializer;

import org.density.core.common.bytes.BytesArray;
import org.density.core.common.bytes.BytesReference;

import java.util.Arrays;

/**
 * A serializer which transforms BytesReference to byte[].
 * The type of BytesReference is NOT preserved after deserialization, but nothing in density should care.
 */
public class BytesReferenceSerializer implements Serializer<BytesReference, byte[]> {
    // This class does not get passed to ehcache itself, so it's not required that classes match after deserialization.

    public BytesReferenceSerializer() {}

    @Override
    public byte[] serialize(BytesReference object) {
        return BytesReference.toBytesWithoutCompact(object);
    }

    @Override
    public BytesReference deserialize(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        return new BytesArray(bytes);
    }

    @Override
    public boolean equals(BytesReference object, byte[] bytes) {
        return Arrays.equals(serialize(object), bytes);
    }
}
