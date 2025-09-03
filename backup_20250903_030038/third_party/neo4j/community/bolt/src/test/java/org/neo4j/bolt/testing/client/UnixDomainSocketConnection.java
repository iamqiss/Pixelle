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
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollDomainSocketChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.kqueue.KQueueDomainSocketChannel;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.unix.DomainSocketAddress;
import java.net.SocketAddress;

public final class UnixDomainSocketConnection extends AbstractNettyConnection {

    private static final Factory factory = new Factory();

    private final DomainSocketAddress address;

    public UnixDomainSocketConnection(DomainSocketAddress address) {
        super(selectEventLoopGroup());
        this.address = address;
    }

    public static BoltTestConnection.Factory factory() {
        return factory;
    }

    private static EventLoopGroup selectEventLoopGroup() {
        if (Epoll.isAvailable()) {
            return new EpollEventLoopGroup(1);
        }
        if (KQueue.isAvailable()) {
            return new KQueueEventLoopGroup(1);
        }

        throw new IllegalStateException(
                "UNIX domain sockets are not available within the current execution environment");
    }

    @Override
    protected DomainSocketAddress address() {
        return this.address;
    }

    @Override
    protected Class<? extends Channel> channelType() {
        if (Epoll.isAvailable()) {
            return EpollDomainSocketChannel.class;
        }
        if (KQueue.isAvailable()) {
            return KQueueDomainSocketChannel.class;
        }

        // unreachable as same check occurs within constructor
        throw new UnsupportedOperationException();
    }

    private static class Factory implements BoltTestConnection.Factory {

        @Override
        public BoltTestConnection create(SocketAddress address) {
            if (address instanceof DomainSocketAddress domainSocketAddress) {
                return new UnixDomainSocketConnection(domainSocketAddress);
            }

            throw new IllegalArgumentException("Cannot initialize unix domain socket connection with address of type "
                    + address.getClass().getSimpleName());
        }

        @Override
        public String toString() {
            return "Unix Domain Socket";
        }
    }
}
