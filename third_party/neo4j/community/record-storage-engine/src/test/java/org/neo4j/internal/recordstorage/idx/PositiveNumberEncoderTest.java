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
package org.neo4j.internal.recordstorage.idx;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.internal.recordstorage.idx.PositiveNumberEncoder.readNumber;
import static org.neo4j.internal.recordstorage.idx.PositiveNumberEncoder.writeNumber;

import java.io.IOException;
import java.util.stream.LongStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.kernel.impl.transaction.log.InMemoryClosableChannel;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

@ExtendWith(RandomExtension.class)
class PositiveNumberEncoderTest {
    @Inject
    private RandomSupport random;

    private final InMemoryClosableChannel channel = new InMemoryClosableChannel();

    @ParameterizedTest
    @MethodSource("getSupported")
    void testPredefinedSupportedValues(long number) throws IOException {
        writeNumber(channel, number);
        long read = readNumber(channel);
        assertEquals(number, read);
    }

    @Test
    void testRandomSupportedValues() throws IOException {
        for (int i = 0; i < 10_000; i++) {
            channel.reset();
            // since we do something special only with numbers less
            // than 0x08 00 00 00 00 if we didn't restrict the generator,
            // the vast majority of the generated numbers would not trigger
            // anything interesting
            long number = random.nextLong(0x08_00_00_00_FFL);
            writeNumber(channel, number);
            long read = readNumber(channel);
            assertEquals(number, read);
        }
    }

    @Test
    void testRandomValues() throws IOException {
        for (int i = 0; i < 10_000; i++) {
            channel.reset();
            long number = random.nextLong(0, 0xF800_0000_0000_0000L);
            writeNumber(channel, number);
            long read = readNumber(channel);
            assertEquals(number, read);
        }
    }

    static LongStream getSupported() {
        return LongStream.of(
                0,
                1,
                127, // last 1 byte number
                128, // first 2 byte number
                0x4000L - 1, // last 2 byte number
                0x4000L, // first 3 byte number
                0x20_0000L - 1, // last 3 byte number
                0x20_0000L, // first 4 byte number
                0x1000_0000L - 1, // last 4 byte number
                0x1000_0000L, // first 5 byte number
                0x08_0000_0000L - 1L, // last 5 byte number
                0x08_0000_0000L);
    }
}
