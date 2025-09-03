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

import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.InstanceOfAssertFactory;
import org.assertj.core.data.Index;
import org.neo4j.bolt.negotiation.util.BitMask;

public final class BitMaskAssertions extends AbstractAssert<BitMaskAssertions, BitMask> {

    private BitMaskAssertions(BitMask bitMask) {
        super(bitMask, BitMaskAssertions.class);
    }

    public static BitMaskAssertions assertThat(BitMask actual) {
        return new BitMaskAssertions(actual);
    }

    public static InstanceOfAssertFactory<BitMask, BitMaskAssertions> bitMask() {
        return new InstanceOfAssertFactory<>(BitMask.class, BitMaskAssertions::assertThat);
    }

    public BitMaskAssertions hasLength(int expected) {
        this.isNotNull();

        var actual = this.actual.length();
        if (actual != expected) {
            this.failWithActualExpectedAndMessage(
                    actual, expected, "Expected exactly <%d> bits but got <%d>", expected, actual);
        }

        return this;
    }

    public BitMaskAssertions hasRemaining(int expected) {
        this.isNotNull();

        var actual = this.actual.readable();
        if (actual != expected) {
            this.failWithActualExpectedAndMessage(
                    actual, expected, "Expected exactly <%d> readable bits but got <%d>", expected, actual);
        }

        return this;
    }

    public BitMaskAssertions hasAtLeastRemaining(int expected) {
        this.isNotNull();

        var actual = this.actual.readable();
        if (actual < expected) {
            this.failWithActualExpectedAndMessage(
                    actual, expected, "Expected at least <%d> readable bits but got <%d>", expected, actual);
        }

        return this;
    }

    public BitMaskAssertions hasAtMostRemaining(int expected) {
        this.isNotNull();

        var actual = this.actual.readable();
        if (this.actual.readable() > expected) {
            this.failWithActualExpectedAndMessage(
                    actual, expected, "Expected at most <%d> readable bits but got <%d>", expected, actual);
        }

        return this;
    }

    public BitMaskAssertions hasBit(boolean expected) {
        this.isNotNull();

        if (!this.actual.isReadable()) {
            this.failWithMessage("Expected at least one remaining readable bit");
        }

        var actual = this.actual.read();
        if (actual != expected) {
            this.failWithActualExpectedAndMessage(
                    actual, expected, "Expected next bit to be <%b> but got <%b>", expected, actual);
        }

        return this;
    }

    public BitMaskAssertions hasBit(boolean expected, Index index) {
        this.isNotNull();

        if (index.value >= this.actual.length()) {
            this.failWithActualExpectedAndMessage(
                    this.actual.length(),
                    index.value,
                    "Expected bit to be present at <%d> but got <%d> bits in total",
                    index.value,
                    this.actual.length());
        }

        var actual = this.actual.get(index.value);
        if (actual != expected) {
            this.failWithActualExpectedAndMessage(
                    actual, expected, "Expected bit at %d to be <%b> but got <%b>", index.value, expected, actual);
        }

        return this;
    }

    public BitMaskAssertions hasBits(int expected, int length) {
        this.isNotNull();

        if (!this.actual.isReadable(length)) {
            this.failWithActualExpectedAndMessage(
                    this.actual.readable(),
                    length,
                    "Expected <%d> readable bits but got <%d>",
                    length,
                    this.actual.readable());
        }

        var actual = this.actual.readN(length);
        if (actual != expected) {
            this.failWithActualExpectedAndMessage(
                    actual,
                    expected,
                    "Expected mask <0x%0" + length + "X> but got <0x%0" + length + "X>",
                    expected,
                    actual);
        }

        return this;
    }

    public BitMaskAssertions isWritable(int length) {
        this.isNotNull();

        if (!this.actual.isWritable(length)) {
            this.failWithActualExpectedAndMessage(
                    this.actual.writable(),
                    length,
                    "Expected <%d> writable bits to remain but got <%d>",
                    length,
                    this.actual.writable());
        }

        return this;
    }
}
