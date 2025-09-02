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
import org.density.common.settings.Settings;
import org.density.common.util.concurrent.ThreadContext;
import org.density.core.common.io.stream.BytesStreamInput;
import org.density.test.DensityTestCase;
import org.density.transport.TestRequest;

import java.io.IOException;

public class NativeOutboundMessageTests extends DensityTestCase {

    public void testNativeOutboundMessageRequestSerialization() throws IOException {
        NativeOutboundMessage.Request message = new NativeOutboundMessage.Request(
            new ThreadContext(Settings.EMPTY),
            new String[0],
            new TestRequest("content"),
            Version.CURRENT,
            "action",
            1,
            false,
            false
        );
        BytesStreamOutput output = new BytesStreamOutput();
        message.serialize(output);

        BytesStreamInput input = new BytesStreamInput(output.bytes().toBytesRef().bytes);
        assertEquals(Version.CURRENT, input.getVersion());
        // reading header details
        assertEquals((byte) 'E', input.readByte());
        assertEquals((byte) 'S', input.readByte());
        assertNotEquals(0, input.readInt());
        assertEquals(1, input.readLong());
        assertEquals(0, input.readByte());
        assertEquals(Version.CURRENT.id, input.readInt());
        int variableHeaderSize = input.readInt();
        assertNotEquals(-1, variableHeaderSize);
    }

}
