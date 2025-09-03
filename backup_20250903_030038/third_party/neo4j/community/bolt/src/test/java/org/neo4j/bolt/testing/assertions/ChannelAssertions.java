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
package org.neo4j.bolt.testing.assertions;

import io.netty.channel.Channel;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.InstanceOfAssertFactory;

public final class ChannelAssertions extends AbstractAssert<ChannelAssertions, Channel> {

    private ChannelAssertions(Channel channel) {
        super(channel, ChannelAssertions.class);
    }

    public static ChannelAssertions assertThat(Channel actual) {
        return new ChannelAssertions(actual);
    }

    public static InstanceOfAssertFactory<Channel, ChannelAssertions> channel() {
        return new InstanceOfAssertFactory<>(Channel.class, ChannelAssertions::assertThat);
    }

    public ChannelAssertions isActive(boolean expected) {
        this.isNotNull();

        if (this.actual.isActive() != expected) {
            this.failWithActualExpectedAndMessage(
                    this.actual.isActive(),
                    expected,
                    expected
                            ? "Expected channel to be active but was inactive"
                            : "Expected channel to be inactive but was active");
        }

        return this;
    }

    public ChannelAssertions isActive() {
        return this.isActive(true);
    }

    public ChannelAssertions isInactive() {
        return this.isActive(false);
    }

    public ChannelAssertions isOpen(boolean expected) {
        this.isNotNull();

        if (this.actual.isOpen() != expected) {
            this.failWithActualExpectedAndMessage(
                    this.actual.isOpen(),
                    expected,
                    expected
                            ? "Expected channel to be open but was closed"
                            : "Expected channel to be closed but was open");
        }

        return this;
    }

    public ChannelAssertions isOpen() {
        return this.isOpen(true);
    }

    public ChannelAssertions isClosed() {
        return this.isOpen(false);
    }
}
