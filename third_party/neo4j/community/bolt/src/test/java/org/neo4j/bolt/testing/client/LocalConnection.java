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
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import java.net.SocketAddress;

public final class LocalConnection extends AbstractNettyConnection {

    private static final Factory factory = new Factory();

    private final LocalAddress address;

    public LocalConnection(LocalAddress address) {
        super();
        this.address = address;
    }

    public static BoltTestConnection.Factory factory() {
        return factory;
    }

    @Override
    protected LocalAddress address() {
        return this.address;
    }

    @Override
    protected Class<? extends Channel> channelType() {
        return LocalChannel.class;
    }

    private static class Factory implements BoltTestConnection.Factory {

        @Override
        public BoltTestConnection create(SocketAddress address) {
            if (address instanceof LocalAddress localAddress) {
                return new LocalConnection(localAddress);
            }

            throw new IllegalArgumentException("Cannot initialize local connection with address of type "
                    + address.getClass().getSimpleName());
        }

        @Override
        public String toString() {
            return "Local Channel";
        }
    }
}
