/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.arrow.flight.transport;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.Location;
import org.density.Version;
import org.density.arrow.flight.bootstrap.ServerConfig;
import org.density.arrow.flight.stats.FlightStatsCollector;
import org.density.cluster.node.DiscoveryNode;
import org.density.common.network.NetworkService;
import org.density.common.settings.Settings;
import org.density.common.util.PageCacheRecycler;
import org.density.core.common.io.stream.NamedWriteableRegistry;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;
import org.density.core.common.transport.BoundTransportAddress;
import org.density.core.common.transport.TransportAddress;
import org.density.core.indices.breaker.NoneCircuitBreakerService;
import org.density.core.transport.TransportResponse;
import org.density.tasks.TaskManager;
import org.density.telemetry.tracing.Tracer;
import org.density.test.DensityTestCase;
import org.density.threadpool.ThreadPool;
import org.density.transport.StreamTransportService;
import org.density.transport.Transport;
import org.density.transport.TransportMessageListener;
import org.density.transport.TransportRequest;
import org.density.transport.TransportService;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public abstract class FlightTransportTestBase extends DensityTestCase {

    private static final AtomicInteger portCounter = new AtomicInteger(0);

    protected DiscoveryNode remoteNode;
    protected Location serverLocation;
    protected HeaderContext headerContext;
    protected ThreadPool threadPool;
    protected NamedWriteableRegistry namedWriteableRegistry;
    protected FlightStatsCollector statsCollector;
    protected BoundTransportAddress boundAddress;
    protected FlightTransport flightTransport;
    protected StreamTransportService streamTransportService;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();

        int basePort = getBasePort(9500);
        int streamPort = basePort + portCounter.incrementAndGet();
        int transportPort = basePort + portCounter.incrementAndGet();

        TransportAddress streamAddress = new TransportAddress(InetAddress.getLoopbackAddress(), streamPort);
        TransportAddress transportAddress = new TransportAddress(InetAddress.getLoopbackAddress(), transportPort);
        remoteNode = new DiscoveryNode(new DiscoveryNode("test-node-id", transportAddress, Version.CURRENT), streamAddress);
        boundAddress = new BoundTransportAddress(new TransportAddress[] { transportAddress }, transportAddress);
        serverLocation = Location.forGrpcInsecure("localhost", streamPort);
        headerContext = new HeaderContext();

        Settings settings = Settings.builder()
            .put("node.name", getTestName())
            .put("aux.transport.transport-flight.port", streamPort)
            .build();
        ServerConfig.init(settings);
        threadPool = new ThreadPool(
            settings,
            ServerConfig.getClientExecutorBuilder(),
            ServerConfig.getGrpcExecutorBuilder(),
            ServerConfig.getServerExecutorBuilder()
        );
        namedWriteableRegistry = new NamedWriteableRegistry(Collections.emptyList());
        statsCollector = new FlightStatsCollector();

        flightTransport = new FlightTransport(
            settings,
            Version.CURRENT,
            threadPool,
            new PageCacheRecycler(settings),
            new NoneCircuitBreakerService(),
            namedWriteableRegistry,
            new NetworkService(Collections.emptyList()),
            mock(Tracer.class),
            null,
            statsCollector
        );
        flightTransport.start();
        TransportService transportService = mock(TransportService.class);
        when(transportService.getTaskManager()).thenReturn(mock(TaskManager.class));
        streamTransportService = spy(
            new StreamTransportService(
                settings,
                flightTransport,
                threadPool,
                StreamTransportService.NOOP_TRANSPORT_INTERCEPTOR,
                x -> remoteNode,
                null,
                transportService.getTaskManager(),
                null,
                mock(Tracer.class)
            )
        );
        streamTransportService.connectToNode(remoteNode);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        if (streamTransportService != null) {
            streamTransportService.close();
        }
        if (flightTransport != null) {
            flightTransport.close();
        }
        if (threadPool != null) {
            threadPool.shutdown();
        }
        super.tearDown();
    }

    protected FlightClientChannel createChannel(FlightClient flightClient) {
        return createChannel(flightClient, threadPool, flightTransport.getResponseHandlers());
    }

    protected FlightClientChannel createChannel(FlightClient flightClient, ThreadPool threadPool) {
        return createChannel(flightClient, threadPool, flightTransport.getResponseHandlers());
    }

    protected FlightClientChannel createChannel(
        FlightClient flightClient,
        ThreadPool customThreadPool,
        Transport.ResponseHandlers handlers
    ) {
        return new FlightClientChannel(
            boundAddress,
            flightClient,
            remoteNode,
            serverLocation,
            headerContext,
            "test-profile",
            handlers,
            customThreadPool,
            new TransportMessageListener() {
            },
            namedWriteableRegistry,
            statsCollector,
            new FlightTransportConfig()
        );
    }

    protected static class TestRequest extends TransportRequest {
        public TestRequest() {}

        public TestRequest(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }
    }

    protected static class TestResponse extends TransportResponse {
        private final String data;

        public TestResponse(String data) {
            this.data = data;
        }

        public TestResponse(StreamInput in) throws IOException {
            super(in);
            this.data = in.readOptionalString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(data);
        }

        public String getData() {
            return data;
        }
    }
}
