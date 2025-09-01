/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.arrow.flight.transport;

import org.density.Version;
import org.density.common.util.BigArrays;
import org.density.core.common.io.stream.NamedWriteableRegistry;
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

class FlightInboundHandler extends InboundHandler {

    public FlightInboundHandler(
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
            keepAlive,
            requestHandlers,
            responseHandlers,
            tracer
        );
    }

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
            new FlightMessageHandler(
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
            )
        );
    }
}
