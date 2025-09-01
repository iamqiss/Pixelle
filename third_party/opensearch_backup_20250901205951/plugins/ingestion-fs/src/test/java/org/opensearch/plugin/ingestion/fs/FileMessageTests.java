/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.plugin.ingestion.fs;

import org.density.test.DensityTestCase;
import org.junit.Assert;

public class FileMessageTests extends DensityTestCase {
    public void testConstructorAndGetters() {
        byte[] payload = { 1, 2, 3 };
        FileMessage message = new FileMessage(payload, 1000L);

        Assert.assertArrayEquals("Payload should be correctly initialized and returned", payload, message.getPayload());
        Assert.assertEquals(1000L, message.getTimestamp().longValue());
    }

    public void testConstructorWithNullPayload() {
        FileMessage message = new FileMessage(null, null);

        Assert.assertNull("Payload should be null", message.getPayload());
        Assert.assertNull("Timestamp should be null", message.getTimestamp());
    }
}
