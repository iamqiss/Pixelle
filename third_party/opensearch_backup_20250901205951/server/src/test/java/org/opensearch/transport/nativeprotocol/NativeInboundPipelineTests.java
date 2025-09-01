/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.transport.nativeprotocol;

import org.density.Version;
import org.density.common.io.stream.BytesStreamOutput;
import org.density.core.common.bytes.BytesReference;
import org.density.transport.InboundPipelineTests;
import org.density.transport.TestRequest;
import org.density.transport.TestResponse;

import java.io.IOException;
import java.util.Collections;

public class NativeInboundPipelineTests extends InboundPipelineTests {

    @Override
    protected BytesReference serialize(
        boolean isRequest,
        Version version,
        boolean handshake,
        boolean compress,
        String action,
        long requestId,
        String value
    ) throws IOException {
        NativeOutboundMessage message;
        if (isRequest) {
            message = new NativeOutboundMessage.Request(
                threadContext,
                new String[0],
                new TestRequest(value),
                version,
                action,
                requestId,
                handshake,
                compress
            );
        } else {
            message = new NativeOutboundMessage.Response(
                threadContext,
                Collections.emptySet(),
                new TestResponse(value),
                version,
                requestId,
                handshake,
                compress
            );
        }

        return message.serialize(new BytesStreamOutput());
    }

}
