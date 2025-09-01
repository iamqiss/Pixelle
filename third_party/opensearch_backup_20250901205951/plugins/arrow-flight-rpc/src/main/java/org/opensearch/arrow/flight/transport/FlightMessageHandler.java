/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.arrow.flight.transport;

import org.density.Version;
import org.density.common.lease.Releasable;
import org.density.common.util.BigArrays;
import org.density.core.common.io.stream.NamedWriteableRegistry;
import org.density.telemetry.tracing.Tracer;
import org.density.threadpool.ThreadPool;
import org.density.transport.Header;
import org.density.transport.NativeMessageHandler;
import org.density.transport.OutboundHandler;
import org.density.transport.ProtocolOutboundHandler;
import org.density.transport.StatsTracker;
import org.density.transport.TcpChannel;
import org.density.transport.TcpTransportChannel;
import org.density.transport.Transport;
import org.density.transport.TransportHandshaker;
import org.density.transport.TransportKeepAlive;

class FlightMessageHandler extends NativeMessageHandler {

    public FlightMessageHandler(
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
        super(
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
            keepAlive
        );
    }

    @Override
    protected ProtocolOutboundHandler createNativeOutboundHandler(
        String nodeName,
        Version version,
        String[] features,
        StatsTracker statsTracker,
        ThreadPool threadPool,
        BigArrays bigArrays,
        OutboundHandler outboundHandler
    ) {
        return new FlightOutboundHandler(nodeName, version, features, statsTracker, threadPool);
    }

    @Override
    protected TcpTransportChannel createTcpTransportChannel(
        ProtocolOutboundHandler outboundHandler,
        TcpChannel channel,
        String action,
        long requestId,
        Version version,
        Header header,
        Releasable breakerRelease
    ) {
        return new FlightTransportChannel(
            (FlightOutboundHandler) outboundHandler,
            channel,
            action,
            requestId,
            version,
            header.getFeatures(),
            header.isCompressed(),
            header.isHandshake(),
            breakerRelease
        );
    }
}
