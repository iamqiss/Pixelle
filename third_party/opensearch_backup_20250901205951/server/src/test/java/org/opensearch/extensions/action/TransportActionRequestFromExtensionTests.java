/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.extensions.action;

import com.google.protobuf.ByteString;
import org.density.common.io.stream.BytesStreamOutput;
import org.density.core.common.bytes.BytesReference;
import org.density.core.common.io.stream.BytesStreamInput;
import org.density.test.DensityTestCase;

public class TransportActionRequestFromExtensionTests extends DensityTestCase {
    public void testTransportActionRequestFromExtension() throws Exception {
        String expectedAction = "test-action";
        ByteString expectedRequestBytes = ByteString.copyFromUtf8("request-bytes");
        String uniqueId = "test-uniqueId";
        TransportActionRequestFromExtension request = new TransportActionRequestFromExtension(
            expectedAction,
            expectedRequestBytes,
            uniqueId
        );

        assertEquals(expectedAction, request.getAction());
        assertEquals(expectedRequestBytes, request.getRequestBytes());
        assertEquals(uniqueId, request.getUniqueId());

        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()));
        request = new TransportActionRequestFromExtension(in);

        assertEquals(expectedAction, request.getAction());
        assertEquals(expectedRequestBytes, request.getRequestBytes());
        assertEquals(uniqueId, request.getUniqueId());
    }
}
