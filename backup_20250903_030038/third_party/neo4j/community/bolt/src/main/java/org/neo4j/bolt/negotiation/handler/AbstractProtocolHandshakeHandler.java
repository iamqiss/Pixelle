/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.negotiation.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.DecoderException;
import java.util.Set;
import org.neo4j.bolt.negotiation.message.ProtocolCapability;
import org.neo4j.bolt.protocol.common.BoltProtocol;
import org.neo4j.bolt.protocol.common.codec.BoltStructEncoder;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.connector.netty.AbstractNettyConnector;
import org.neo4j.bolt.protocol.common.handler.HouseKeeperHandler;
import org.neo4j.bolt.protocol.common.handler.ProtocolLoggingHandler;
import org.neo4j.bolt.protocol.common.handler.RequestHandler;
import org.neo4j.bolt.protocol.common.handler.StateSignalFilterHandler;
import org.neo4j.bolt.protocol.common.handler.messages.GoodbyeMessageHandler;
import org.neo4j.bolt.protocol.common.message.response.ResponseMessage;
import org.neo4j.bolt.runtime.throttle.ChannelReadThrottleHandler;
import org.neo4j.bolt.runtime.throttle.ChannelWriteThrottleHandler;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.packstream.codec.PackstreamStructDecoder;
import org.neo4j.packstream.codec.PackstreamStructEncoder;
import org.neo4j.packstream.codec.transport.ChunkFrameDecoder;
import org.neo4j.packstream.codec.transport.ChunkFrameEncoder;
import org.neo4j.packstream.codec.transport.FrameSignalEncoder;

public abstract sealed class AbstractProtocolHandshakeHandler<I> extends SimpleChannelInboundHandler<I>
        permits ModernProtocolHandshakeHandler, LegacyProtocolHandshakeHandler {

    protected final InternalLogProvider logging;
    protected final InternalLog log;

    protected AbstractNettyConnector<?> connector;
    protected Connection connection;

    protected AbstractProtocolHandshakeHandler(InternalLogProvider logging) {
        this.logging = logging;
        this.log = logging.getLog(this.getClass());
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        this.connection = Connection.getConnection(ctx.channel());
        this.connector = (AbstractNettyConnector<?>) this.connection.connector();
    }

    public final void finalizeHandshake(
            ChannelHandlerContext ctx, BoltProtocol protocol, Set<ProtocolCapability> capabilities) {
        // complete handshake by notifying the connection about its new protocol revision and notify the peer about the
        // selected revision
        this.connection.selectProtocol(protocol, capabilities);
        this.onVersionSelected(ctx, protocol);

        // KeepAliveHandler needs the FrameSignalEncoder to send outbound NOOPs
        ctx.pipeline()
                .addLast(new StateSignalFilterHandler())
                .addLast(new FrameSignalEncoder(protocol.frameSignalFilter()));

        var config = this.connector.configuration();
        if (config.enableOutboundBufferThrottle()) {
            ctx.channel()
                    .config()
                    .setWriteBufferWaterMark(new WriteBufferWaterMark(
                            config.outboundBufferThrottleLowWatermark(), config.outboundBufferThrottleHighWatermark()));
        }

        ChunkFrameDecoder frameDecoder;
        var readLimit = config.maxAuthenticationInboundBytes();
        if (readLimit != 0) {
            this.log.debug(
                    "Imposing %d byte read-limit on connection '%s' until authentication is completed",
                    readLimit, this.connection.id());
            frameDecoder = new ChunkFrameDecoder(readLimit, this.logging);
        } else {
            frameDecoder = new ChunkFrameDecoder(this.logging);
        }

        if (config.enableMergeCumulator()) {
            this.log.warn("Enabling merge cumulator for chunk decoding - Network performance may be degraded");
            frameDecoder.setCumulator(ByteToMessageDecoder.MERGE_CUMULATOR);
        }
        ctx.pipeline().addLast(ChunkFrameDecoder.NAME, frameDecoder);

        // if raw protocol logging is enabled, we'll remove the previous handler and position it
        // after the chunk decoder handlers in order to split up the continuous byte stream into
        // coherent messages before passing them to the log
        if (config.enableProtocolLogging() && config.protocolLoggingMode().isLoggingRawTraffic()) {
            ctx.pipeline().remove(ProtocolLoggingHandler.RAW_NAME);
            ctx.pipeline().addLast(ProtocolLoggingHandler.RAW_NAME, new ProtocolLoggingHandler(this.logging));
        }

        ctx.pipeline()
                .addLast("chunkFrameEncoder", new ChunkFrameEncoder())
                .addLast(
                        "structDecoder",
                        new PackstreamStructDecoder<>(connection, protocol.requestMessageRegistry(), logging))
                .addLast(
                        "structEncoder",
                        new PackstreamStructEncoder<>(
                                ResponseMessage.class, connection, protocol.responseMessageRegistry()));

        var inboundMessageThrottleHighWatermark = config.inboundBufferThrottleHighWatermark();
        if (inboundMessageThrottleHighWatermark != 0) {
            ctx.pipeline()
                    .addLast(
                            "readThrottleHandler",
                            new ChannelReadThrottleHandler(
                                    config.inboundBufferThrottleLowWatermark(),
                                    inboundMessageThrottleHighWatermark,
                                    logging));
        }

        // if logging of decoded messages is enabled, we'll discard the old handler and introduce a
        // new instance at the correct position within the pipeline
        if (config.enableProtocolLogging() && config.protocolLoggingMode().isLoggingDecodedTraffic()) {
            ctx.pipeline().remove(ProtocolLoggingHandler.DECODED_NAME);
            ctx.pipeline().addLast(ProtocolLoggingHandler.DECODED_NAME, new ProtocolLoggingHandler(this.logging));
        }

        ctx.pipeline()
                .addLast(GoodbyeMessageHandler.HANDLER_NAME, new GoodbyeMessageHandler(logging))
                .addLast("boltStructEncoder", new BoltStructEncoder());

        var writeThrottleEnabled = config.enableOutboundBufferThrottle();
        var writeTimeoutMillis = config.outboundBufferMaxThrottleDuration().toMillis();
        if (writeThrottleEnabled && writeTimeoutMillis != 0) {
            ctx.pipeline()
                    .addLast(
                            "channelThrottleHandler",
                            new ChannelWriteThrottleHandler(writeTimeoutMillis, this.logging));
        }

        ctx.pipeline()
                .addLast("requestHandler", new RequestHandler(logging))
                .addLast(HouseKeeperHandler.HANDLER_NAME, new HouseKeeperHandler(logging));

        this.removeStageHandlers(ctx);

        this.connection.notifyListeners(listener -> listener.onProtocolSelected(protocol));
    }

    protected void onVersionSelected(ChannelHandlerContext ctx, BoltProtocol protocol) {}

    protected void removeStageHandlers(ChannelHandlerContext ctx) {
        ctx.pipeline().remove(this);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if (cause instanceof DecoderException) {
            // we'll handle decoder exceptions silently as they indicate an issue with the negotiation
            // payload (aka a client is speaking a different protocol)
            log.debug("Failed Bolt handshake: Malformed negotiation payload received.", cause);
        } else {
            // all other exceptions are logged properly as they are very likely bugs within the handler
            // logic
            log.error("Fatal error occurred during protocol handshaking: " + ctx.channel(), cause);
        }

        ctx.close();
    }
}
