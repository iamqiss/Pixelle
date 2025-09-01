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
import org.density.core.common.io.stream.Writeable;
import org.density.transport.InboundDecoderTests;

import java.io.IOException;
import java.util.Collections;

public class NativeInboundDecoderTests extends InboundDecoderTests {

    @Override
    protected BytesReference serialize(
        boolean isRequest,
        Version version,
        boolean handshake,
        boolean compress,
        String action,
        long requestId,
        Writeable transportMessage
    ) throws IOException {
        NativeOutboundMessage message;
        if (isRequest) {
            message = new NativeOutboundMessage.Request(
                threadContext,
                new String[0],
                transportMessage,
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
                transportMessage,
                version,
                requestId,
                handshake,
                compress
            );
        }

        return message.serialize(new BytesStreamOutput());
    }

}
