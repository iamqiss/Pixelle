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
package org.neo4j.bolt.negotiation.codec;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderException;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.bolt.negotiation.ProtocolVersion;
import org.neo4j.bolt.negotiation.message.ModernProtocolNegotiationFinalizeMessage;
import org.neo4j.bolt.negotiation.message.ProtocolCapability;

class ModernProtocolNegotiationFinalizeMessageDecoderTest {

    private EmbeddedChannel channel;

    @BeforeEach
    void prepare() {
        this.channel = new EmbeddedChannel(new ModernProtocolNegotiationFinalizeMessageDecoder());
    }

    @Test
    void shouldDecodeValidPayloads() {
        var buf = Unpooled.buffer()
                .writeInt(new ProtocolVersion(5, 0).encode())
                .writeByte(0b10000001)
                .writeByte(0b00000000);

        this.channel.writeInbound(buf);

        var message = this.channel.readInbound();

        Assertions.assertThat(message)
                .isNotNull()
                .isInstanceOfSatisfying(ModernProtocolNegotiationFinalizeMessage.class, m -> {
                    Assertions.assertThat(m.selectedVersion()).isEqualTo(new ProtocolVersion(5, 0));

                    Assertions.assertThat(m.capabilities()).contains(ProtocolCapability.FABRIC);
                });
    }

    @Test
    void shouldDecodeSplitPayloads() {
        var buf = Unpooled.buffer()
                .writeInt(new ProtocolVersion(5, 0).encode())
                .writeByte(0b10000001)
                .writeByte(0b00000000);

        this.channel.writeInbound(buf.readRetainedSlice(2));

        Assertions.assertThat((Object) this.channel.readInbound()).isNull();

        this.channel.writeInbound(buf.readRetainedSlice(2));

        Assertions.assertThat((Object) this.channel.readInbound()).isNull();

        this.channel.writeInbound(buf);

        var message = this.channel.readInbound();

        Assertions.assertThat(message)
                .isNotNull()
                .isInstanceOfSatisfying(ModernProtocolNegotiationFinalizeMessage.class, m -> {
                    Assertions.assertThat(m.selectedVersion()).isEqualTo(new ProtocolVersion(5, 0));

                    Assertions.assertThat(m.capabilities()).contains(ProtocolCapability.FABRIC);
                });
    }

    @Test
    void shouldIgnoreTruncatedPayloads() {
        this.channel.writeInbound(Unpooled.buffer().writeByte(0));

        Assertions.assertThat((Object) this.channel.readInbound()).isNull();

        this.channel.writeInbound(Unpooled.buffer().writeByte(0).writeByte(0).writeByte(0));

        Assertions.assertThat((Object) this.channel.readInbound()).isNull();
    }

    @Test
    void shouldFailWhenRangeIsGiven() {
        var buf = Unpooled.buffer()
                .writeInt(new ProtocolVersion(5, 4, 3).encode())
                .writeByte(0b00000000);

        Assertions.assertThatExceptionOfType(DecoderException.class)
                .isThrownBy(() -> this.channel.writeInbound(buf))
                .withCauseInstanceOf(IllegalArgumentException.class)
                .withMessageContaining("Illegal version selection: Selection cannot include range");

        this.channel.writeInbound(buf);
    }
}
