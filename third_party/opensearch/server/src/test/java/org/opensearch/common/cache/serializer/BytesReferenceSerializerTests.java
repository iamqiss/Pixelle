/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.common.cache.serializer;

import org.density.common.Randomness;
import org.density.common.bytes.ReleasableBytesReference;
import org.density.common.util.BigArrays;
import org.density.common.util.PageCacheRecycler;
import org.density.core.common.bytes.BytesArray;
import org.density.core.common.bytes.BytesReference;
import org.density.core.common.bytes.CompositeBytesReference;
import org.density.core.common.util.ByteArray;
import org.density.test.DensityTestCase;

import java.util.Random;

public class BytesReferenceSerializerTests extends DensityTestCase {
    public void testEquality() throws Exception {
        BytesReferenceSerializer ser = new BytesReferenceSerializer();
        // Test that values are equal before and after serialization, for each implementation of BytesReference.
        byte[] bytesValue = new byte[1000];
        Random rand = Randomness.get();
        rand.nextBytes(bytesValue);

        BytesReference ba = new BytesArray(bytesValue);
        byte[] serialized = ser.serialize(ba);
        assertTrue(ser.equals(ba, serialized));
        BytesReference deserialized = ser.deserialize(serialized);
        assertEquals(ba, deserialized);

        ba = new BytesArray(new byte[] {});
        serialized = ser.serialize(ba);
        assertTrue(ser.equals(ba, serialized));
        deserialized = ser.deserialize(serialized);
        assertEquals(ba, deserialized);

        BytesReference cbr = CompositeBytesReference.of(new BytesArray(bytesValue), new BytesArray(bytesValue));
        serialized = ser.serialize(cbr);
        assertTrue(ser.equals(cbr, serialized));
        deserialized = ser.deserialize(serialized);
        assertEquals(cbr, deserialized);

        // We need the PagedBytesReference to be larger than the page size (16 KB) in order to actually create it
        byte[] pbrValue = new byte[PageCacheRecycler.PAGE_SIZE_IN_BYTES * 2];
        rand.nextBytes(pbrValue);
        ByteArray arr = BigArrays.NON_RECYCLING_INSTANCE.newByteArray(pbrValue.length);
        arr.set(0L, pbrValue, 0, pbrValue.length);
        assert !arr.hasArray();
        BytesReference pbr = BytesReference.fromByteArray(arr, pbrValue.length);
        serialized = ser.serialize(pbr);
        assertTrue(ser.equals(pbr, serialized));
        deserialized = ser.deserialize(serialized);
        assertEquals(pbr, deserialized);

        BytesReference rbr = new ReleasableBytesReference(new BytesArray(bytesValue), ReleasableBytesReference.NO_OP);
        serialized = ser.serialize(rbr);
        assertTrue(ser.equals(rbr, serialized));
        deserialized = ser.deserialize(serialized);
        assertEquals(rbr, deserialized);
    }
}
