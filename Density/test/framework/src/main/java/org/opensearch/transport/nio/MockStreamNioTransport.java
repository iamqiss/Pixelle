/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.transport.nio;

import org.density.Version;
import org.density.common.network.NetworkService;
import org.density.common.settings.Settings;
import org.density.common.util.BigArrays;
import org.density.common.util.PageCacheRecycler;
import org.density.core.common.io.stream.NamedWriteableRegistry;
import org.density.core.indices.breaker.CircuitBreakerService;
import org.density.telemetry.tracing.Tracer;
import org.density.threadpool.ThreadPool;
import org.density.transport.InboundHandler;
import org.density.transport.OutboundHandler;
import org.density.transport.ProtocolMessageHandler;
import org.density.transport.StatsTracker;
import org.density.transport.Transport;
import org.density.transport.TransportHandshaker;
import org.density.transport.TransportKeepAlive;
import org.density.transport.TransportProtocol;

import java.util.Map;

/**
 * A specialized MockNioTransport that supports streaming transport channels for testing streaming search.
 * This transport extends MockNioTransport and overrides the inbound handler creation to provide
 * MockNativeMessageHandler which creates mock streaming transport channels when needed.
 *
 * @density.internal
 */
public class MockStreamNioTransport extends MockNioTransport {

    public MockStreamNioTransport(
        Settings settings,
        Version version,
        ThreadPool threadPool,
        NetworkService networkService,
        PageCacheRecycler pageCacheRecycler,
        NamedWriteableRegistry namedWriteableRegistry,
        CircuitBreakerService circuitBreakerService,
        Tracer tracer
    ) {
        super(settings, version, threadPool, networkService, pageCacheRecycler, namedWriteableRegistry, circuitBreakerService, tracer);
    }

    @Override
    protected InboundHandler createInboundHandler(
        String nodeName,
        Version version,
        String[] features,
        StatsTracker statsTracker,
        ThreadPool threadPool,
        BigArrays bigArrays,
        OutboundHandler outboundHandler,
        NamedWriteableRegistry namedWriteableRegistry,
        TransportHandshaker handshaker,
        TransportKeepAlive keepAlive,
        Transport.RequestHandlers requestHandlers,
        Transport.ResponseHandlers responseHandlers,
        Tracer tracer
    ) {
        // Create an InboundHandler that uses our MockNativeMessageHandler
        return new InboundHandler(
            nodeName,
            version,
            features,
            statsTracker,
            threadPool,
            bigArrays,
            outboundHandler,
            namedWriteableRegistry,
            handshaker,
            keepAlive,
            requestHandlers,
            responseHandlers,
            tracer
        ) {
            @Override
            protected Map<TransportProtocol, ProtocolMessageHandler> createProtocolMessageHandlers(
                String nodeName,
                Version version,
                String[] features,
                StatsTracker statsTracker,
                ThreadPool threadPool,
                BigArrays bigArrays,
                OutboundHandler outboundHandler,
                NamedWriteableRegistry namedWriteableRegistry,
                TransportHandshaker handshaker,
                Transport.RequestHandlers requestHandlers,
                Transport.ResponseHandlers responseHandlers,
                Tracer tracer,
                TransportKeepAlive keepAlive
            ) {
                return Map.of(
                    TransportProtocol.NATIVE,
                    new MockNativeMessageHandler(
                        nodeName,
                        version,
                        features,
                        statsTracker,
                        threadPool,
                        bigArrays,
                        outboundHandler,
                        namedWriteableRegistry,
                        handshaker,
                        requestHandlers,
                        responseHandlers,
                        tracer,
                        keepAlive,
                        getMessageListener()
                    )
                );
            }
        };
    }
}
