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

public class ExtensionActionResponseTests extends DensityTestCase {

    public void testExtensionActionResponse() throws Exception {
        byte[] expectedResponseBytes = "response-bytes".getBytes(StandardCharsets.UTF_8);
        ExtensionActionResponse response = new ExtensionActionResponse(expectedResponseBytes);

        assertEquals(expectedResponseBytes, response.getResponseBytes());

        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()));
        response = new ExtensionActionResponse(in);
        assertArrayEquals(expectedResponseBytes, response.getResponseBytes());
    }
}
