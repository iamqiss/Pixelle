/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.example.stream;

import org.density.arrow.flight.transport.FlightStreamPlugin;
import org.density.cluster.node.DiscoveryNode;
import org.density.core.common.io.stream.StreamInput;
import org.density.plugins.Plugin;
import org.density.test.DensityIntegTestCase;
import org.density.threadpool.ThreadPool;
import org.density.transport.StreamTransportResponseHandler;
import org.density.transport.StreamTransportService;
import org.density.transport.TransportException;
import org.density.transport.TransportRequestOptions;
import org.density.transport.stream.StreamTransportResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.density.common.util.FeatureFlags.STREAM_TRANSPORT;

@DensityIntegTestCase.ClusterScope(scope = DensityIntegTestCase.Scope.SUITE, minNumDataNodes = 2, maxNumDataNodes = 2)
public class StreamTransportExampleIT extends DensityIntegTestCase {
    @Override
    public void setUp() throws Exception {
        super.setUp();
        internalCluster().ensureAtLeastNumDataNodes(2);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(StreamTransportExamplePlugin.class, FlightStreamPlugin.class);
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testStreamTransportAction() throws Exception {
        for (DiscoveryNode node : getClusterState().nodes()) {
            StreamTransportService streamTransportService = internalCluster().getInstance(StreamTransportService.class);

            List<StreamDataResponse> responses = new ArrayList<>();
            CountDownLatch latch = new CountDownLatch(1);
            StreamTransportResponseHandler<StreamDataResponse> handler = new StreamTransportResponseHandler<StreamDataResponse>() {
                @Override
                public void handleStreamResponse(StreamTransportResponse<StreamDataResponse> streamResponse) {
                    try {
                        StreamDataResponse response;
                        while ((response = streamResponse.nextResponse()) != null) {
                            responses.add(response);
                        }
                        streamResponse.close();
                        latch.countDown();
                    } catch (Exception e) {
                        streamResponse.cancel("Test error", e);
                        fail("Stream processing failed: " + e.getMessage());
                    }
                }

                @Override
                public void handleException(TransportException exp) {
                    fail("Transport exception: " + exp.getMessage());
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }

                @Override
                public StreamDataResponse read(StreamInput in) throws IOException {
                    return new StreamDataResponse(in);
                }
            };

            StreamDataRequest request = new StreamDataRequest(3, 1);
            streamTransportService.sendRequest(
                node,
                StreamDataAction.NAME,
                request,
                TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STREAM).build(),
                handler
            );
            assertTrue(latch.await(2, TimeUnit.SECONDS));
            // Wait for responses
            assertEquals(3, responses.size());

            assertEquals("Stream data item 1", responses.get(0).getMessage());
            assertEquals("Stream data item 2", responses.get(1).getMessage());
            assertEquals("Stream data item 3", responses.get(2).getMessage());
            assertTrue(responses.get(2).isLast());
        }
    }
}
