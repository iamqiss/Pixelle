/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.extensions.action;

import org.density.common.io.stream.BytesStreamOutput;
import org.density.core.common.bytes.BytesReference;
import org.density.core.common.io.stream.BytesStreamInput;
import org.density.test.DensityTestCase;

import java.nio.charset.StandardCharsets;

public class RemoteExtensionActionResponseTests extends DensityTestCase {

    public void testExtensionActionResponse() throws Exception {
        byte[] expectedResponseBytes = "response-bytes".getBytes(StandardCharsets.UTF_8);
        RemoteExtensionActionResponse response = new RemoteExtensionActionResponse(true, expectedResponseBytes);

        assertTrue(response.isSuccess());
        assertEquals(expectedResponseBytes, response.getResponseBytes());

        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()));
        response = new RemoteExtensionActionResponse(in);

        assertTrue(response.isSuccess());
        assertArrayEquals(expectedResponseBytes, response.getResponseBytes());
    }

    public void testSetters() {
        String expectedResponse = "response-bytes";
        byte[] expectedResponseBytes = expectedResponse.getBytes(StandardCharsets.UTF_8);
        byte[] expectedEmptyBytes = new byte[0];
        RemoteExtensionActionResponse response = new RemoteExtensionActionResponse(false, expectedEmptyBytes);
        assertArrayEquals(expectedEmptyBytes, response.getResponseBytes());
        assertFalse(response.isSuccess());

        response.setResponseBytesAsString(expectedResponse);
        assertArrayEquals(expectedResponseBytes, response.getResponseBytes());

        response.setResponseBytes(expectedResponseBytes);
        assertArrayEquals(expectedResponseBytes, response.getResponseBytes());
        assertEquals(expectedResponse, response.getResponseBytesAsString());

        response.setSuccess(true);
        assertTrue(response.isSuccess());
    }
}
