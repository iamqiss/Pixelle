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
package org.neo4j.bolt.testing.client.handler;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;

public class WebSocketHandler extends ChannelDuplexHandler {
    private final WebSocketClientHandshaker handshaker;
    private ChannelPromise handshakePromise;

    public WebSocketHandler(WebSocketClientHandshaker handshaker) {
        this.handshaker = handshaker;
    }

    public ChannelPromise handshakePromise() {
        if (this.handshakePromise == null) {
            throw new IllegalStateException("Handler has yet to be initialized");
        }

        return this.handshakePromise;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        this.handshakePromise = ctx.newPromise();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        this.handshaker.handshake(ctx.channel());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (!this.handshaker.isHandshakeComplete()) {
            this.handshakePromise.setFailure(cause);
        }

        super.exceptionCaught(ctx, cause);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (!this.handshaker.isHandshakeComplete()) {
            this.handshakePromise.setFailure(new IllegalStateException("Connection closed"));
        }

        super.channelInactive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof FullHttpResponse response) {
            if (!this.handshaker.isHandshakeComplete()) {
                this.handshaker.finishHandshake(ctx.channel(), response);
                this.handshakePromise.setSuccess();
                return;
            }

            throw new IllegalStateException("Received HTTP response during WebSocket session");
        }

        if (msg instanceof CloseWebSocketFrame) {
            ctx.channel().close();
        } else if (msg instanceof TextWebSocketFrame frame) {
            // TODO: Better output representation in logging output
            throw new IllegalStateException(
                    "Received plain text WebSocket frame during binary session: " + frame.text());
        } else if (msg instanceof BinaryWebSocketFrame frame) {
            super.channelRead(ctx, frame.content().retain());
            return;
        }

        throw new IllegalStateException(
                "Received arbitrary message of type " + msg.getClass().getName() + " during WebSocket session");
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof ByteBuf content) {
            msg = new BinaryWebSocketFrame(content.retain());
        }

        super.write(ctx, msg, promise);
    }
}
