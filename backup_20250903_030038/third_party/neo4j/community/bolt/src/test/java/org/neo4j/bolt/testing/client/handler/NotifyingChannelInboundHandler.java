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
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class NotifyingChannelInboundHandler extends SimpleChannelInboundHandler<ByteBuf> {
    private final CompositeByteBuf compositeByteBuf;
    private final Object lock;

    public NotifyingChannelInboundHandler(CompositeByteBuf compositeByteBuf, Object lock) {
        this.compositeByteBuf = compositeByteBuf;
        this.lock = lock;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf) throws Exception {
        synchronized (this.lock) {
            this.compositeByteBuf.addComponent(true, byteBuf.retain());

            this.lock.notifyAll();
        }
    }
}
