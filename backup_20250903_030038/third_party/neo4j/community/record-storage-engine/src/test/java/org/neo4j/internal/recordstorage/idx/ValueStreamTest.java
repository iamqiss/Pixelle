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

import static java.time.ZoneOffset.UTC;
import static org.apache.commons.lang3.RandomStringUtils.randomAscii;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.ZonedDateTime;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.internal.recordstorage.idx.value.PeekableChannel;
import org.neo4j.internal.recordstorage.idx.value.ValueStream;
import org.neo4j.kernel.impl.transaction.log.InMemoryClosableChannel;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

class ValueStreamTest {
    private final InMemoryClosableChannel channel = new InMemoryClosableChannel();

    @ParameterizedTest
    @MethodSource("values")
    void testValues(Value value) throws IOException {
        doTestValue(value);
    }

    private void doTestValue(Value value) throws IOException {
        ValueStream.write(channel, value);
        AnyValue readValue = ValueStream.readValue(new PeekableChannel(channel));
        assertEquals(value, readValue);
    }

    private static Stream<Value> values() {
        return Stream.of(
                Values.NO_VALUE,
                Values.byteValue((byte) 123),
                Values.byteValue(Byte.MAX_VALUE),
                Values.shortValue((short) 123),
                Values.shortValue(Short.MAX_VALUE),
                Values.intValue(123),
                Values.intValue(Integer.MAX_VALUE),
                Values.longValue(123L),
                Values.longValue(Long.MAX_VALUE),
                Values.floatValue(123.123f),
                Values.doubleValue(123.123),
                Values.stringValue(randomAscii(4)),
                Values.stringValue(randomAscii(14)),
                Values.stringValue(randomAscii(24)),
                Values.stringValue(randomAscii(48)),
                Values.stringValue(randomAscii(96)),
                Values.stringValue(randomAscii(188)),
                Values.utf8Value(randomAscii(4)),
                Values.utf8Value(randomAscii(14)),
                Values.utf8Value(randomAscii(24)),
                Values.utf8Value(randomAscii(48)),
                Values.utf8Value(randomAscii(96)),
                Values.utf8Value(randomAscii(188)),
                Values.temporalValue(LocalDate.parse("2021-10-01")),
                Values.temporalValue(LocalDateTime.parse("2021-10-01T10:15")),
                Values.temporalValue(ZonedDateTime.parse("2021-10-01T10:15+01:00[Europe/Stockholm]")),
                Values.floatArray(new float[] {5.0f, 456789.76f}),
                Values.doubleArray(new double[] {1.0, 4556789.76}),
                Values.byteArray("Some text".getBytes(StandardCharsets.UTF_8)),
                Values.longArray(new long[] {12L, 456789L}),
                Values.intArray(new int[] {123, 456789}),
                Values.shortArray(new short[] {3, 4789}),
                Values.booleanArray(new boolean[] {false, true, false}),
                Values.charArray(new char[] {'a', 'c', 'b'}),
                Values.stringArray("aaa", "red", "ö", "чщцю"),
                Values.dateTimeArray(new ZonedDateTime[] {
                    ZonedDateTime.of(1987, 5, 7, 7, 8, 9, 10, UTC), ZonedDateTime.of(2007, 1, 2, 3, 4, 5, 6, UTC)
                }),
                Values.localDateTimeArray(new LocalDateTime[] {
                    LocalDateTime.of(1987, 5, 7, 7, 8, 9, 10), LocalDateTime.of(2007, 1, 2, 3, 4, 5, 6)
                }),
                Values.dateArray(new LocalDate[] {LocalDate.of(1987, 5, 7), LocalDate.of(2007, 1, 2)}),
                Values.localTimeArray(new LocalTime[] {
                    LocalTime.of(10, 11, 12, 13), LocalTime.of(11, 12, 13, 14), LocalTime.of(12, 13, 14, 15),
                }),
                Values.timeArray(new OffsetTime[] {
                    OffsetTime.of(5, 6, 7, 8, UTC), OffsetTime.of(6, 7, 8, 1256, UTC),
                }),
                Values.durationArray(new DurationValue[] {
                    DurationValue.duration(Period.of(5, 0, 0)), DurationValue.duration(2, 0, 0, 0),
                }),
                Values.pointArray(new PointValue[] {
                    PointValue.maxPointValueOf(CoordinateReferenceSystem.CARTESIAN),
                    PointValue.minPointValueOf(CoordinateReferenceSystem.CARTESIAN_3D)
                }));
    }
}
