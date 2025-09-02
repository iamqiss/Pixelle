/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.plugin.ingestion.fs;

import org.density.test.DensityTestCase;

import java.nio.ByteBuffer;

public class FileOffsetTests extends DensityTestCase {

    public void testFileOffset() {
        FileOffset offset = new FileOffset(42L);
        byte[] serialized = offset.serialize();
        long deserialized = ByteBuffer.wrap(serialized).getLong();
        assertEquals(offset.getLine(), deserialized);
    }
}
