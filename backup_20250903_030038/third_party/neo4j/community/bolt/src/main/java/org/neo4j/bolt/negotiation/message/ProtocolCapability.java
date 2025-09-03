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
package org.neo4j.bolt.negotiation.message;

import io.netty.buffer.ByteBufAllocator;
import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.neo4j.bolt.negotiation.util.BitMask;
import org.neo4j.util.Preconditions;

public enum ProtocolCapability {

    /**
     * Handshake v2 describes an implicit capability which is attached to all connections negotiated
     * via the new handshake protocol.
     * <p/>
     * This capability cannot be transmitted via the network and has thus not been assigned an
     * identifier.
     */
    HANDSHAKE_V2,

    /**
     * Fabric describes a connection which has been established via the internal Bolt connector and
     * supports additional protocol functionality unique to this connector.
     */
    FABRIC(0x01);

    private final int networkIndex;

    private static final int maxNetworkIndex;
    private static final ProtocolCapability[] networkIndexMap;

    static {
        maxNetworkIndex = Stream.of(values())
                .mapToInt(capability -> capability.networkIndex)
                .filter(it -> it > 0)
                .max()
                .orElse(-1);

        var map = new ProtocolCapability[maxNetworkIndex];
        for (var capability : values()) {
            // negative values indicate that the server implicitly detects the capability as part
            // of the negotiation process
            if (capability.networkIndex <= 0) {
                continue;
            }

            map[capability.networkIndex - 1] = capability;
        }
        networkIndexMap = map;
    }

    ProtocolCapability(int networkIndex) {
        if (networkIndex == 0 || networkIndex < -1) {
            throw new IllegalArgumentException("Network index must be positive");
        }

        this.networkIndex = networkIndex;
    }

    ProtocolCapability() {
        this(-1);
    }

    public static Optional<ProtocolCapability> byNetworkIndex(int networkIndex) {
        Preconditions.requirePositive(networkIndex);

        if (networkIndex > networkIndexMap.length) {
            return Optional.empty();
        }

        return Optional.ofNullable(networkIndexMap[networkIndex - 1]);
    }

    public static BitMask toBitMask(ByteBufAllocator alloc, Set<ProtocolCapability> capabilities) {
        var mask = new BitMask(alloc, maxNetworkIndex);
        for (var capability : capabilities) {
            if (capability.networkIndex < 1) {
                throw new IllegalArgumentException("Cannot encode implicit capability " + capability + " to BitMask");
            }

            mask.set(capability.networkIndex - 1, true);
        }
        return mask;
    }

    public static Set<ProtocolCapability> fromBitMask(BitMask mask) {
        var capabilities = EnumSet.noneOf(ProtocolCapability.class);
        for (var i = 0; i < mask.length(); ++i) {
            if (i >= networkIndexMap.length) {
                break;
            }
            if (!mask.get(i)) {
                continue;
            }

            var capability = networkIndexMap[i];
            if (capability == null) {
                continue;
            }

            capabilities.add(capability);
        }
        return capabilities;
    }
}
