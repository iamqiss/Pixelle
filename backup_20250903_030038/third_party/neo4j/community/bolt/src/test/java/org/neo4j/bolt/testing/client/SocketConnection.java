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
package org.neo4j.bolt.testing.client;

import io.netty.channel.Channel;
import io.netty.channel.socket.nio.NioSocketChannel;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

public sealed class SocketConnection extends AbstractNettyConnection
        permits SecureSocketConnection, WebSocketConnection {
    private static final Factory factory = new Factory();

    protected final InetSocketAddress address;

    public static BoltTestConnection.Factory factory() {
        return factory;
    }

    public SocketConnection(InetSocketAddress address) {
        super();
        this.address = address;
    }

    @Override
    protected InetSocketAddress address() {
        return this.address;
    }

    @Override
    protected Class<? extends Channel> channelType() {
        return NioSocketChannel.class;
    }

    private static class Factory implements BoltTestConnection.Factory {

        @Override
        public SocketConnection create(SocketAddress address) {
            if (address instanceof InetSocketAddress inetSocketAddress) {
                return new SocketConnection(inetSocketAddress);
            }

            throw new IllegalArgumentException("Cannot initialize socket connection with address of type "
                    + address.getClass().getSimpleName());
        }

        @Override
        public String toString() {
            return "Plain Socket";
        }
    }
}
