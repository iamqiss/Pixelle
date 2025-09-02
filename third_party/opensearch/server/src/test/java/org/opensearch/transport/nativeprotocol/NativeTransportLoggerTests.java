/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.transport.nativeprotocol;

import org.density.Version;
import org.density.action.admin.cluster.stats.ClusterStatsAction;
import org.density.action.admin.cluster.stats.ClusterStatsRequest;
import org.density.common.io.stream.BytesStreamOutput;
import org.density.common.settings.Settings;
import org.density.common.util.concurrent.ThreadContext;
import org.density.core.common.bytes.BytesReference;
import org.density.test.junit.annotations.TestLogging;
import org.density.transport.TransportLoggerTests;

import java.io.IOException;

@TestLogging(value = "org.density.transport.TransportLogger:trace", reason = "to ensure we log network events on TRACE level")
public class NativeTransportLoggerTests extends TransportLoggerTests {

    public BytesReference buildRequest() throws IOException {
        boolean compress = randomBoolean();
        try (BytesStreamOutput bytesStreamOutput = new BytesStreamOutput()) {
            NativeOutboundMessage.Request request = new NativeOutboundMessage.Request(
                new ThreadContext(Settings.EMPTY),
                new String[0],
                new ClusterStatsRequest(),
                Version.CURRENT,
                ClusterStatsAction.NAME,
                randomInt(30),
                false,
                compress
            );
            return request.serialize(new BytesStreamOutput());
        }
    }
}
