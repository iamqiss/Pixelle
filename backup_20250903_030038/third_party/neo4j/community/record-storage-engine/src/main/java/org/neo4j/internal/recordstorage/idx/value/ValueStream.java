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
package org.neo4j.internal.recordstorage.idx.value;

import static java.time.ZoneOffset.UTC;
import static org.neo4j.values.storable.DateTimeValue.datetime;
import static org.neo4j.values.storable.DateValue.date;
import static org.neo4j.values.storable.DateValue.epochDateRaw;
import static org.neo4j.values.storable.DurationValue.duration;
import static org.neo4j.values.storable.LocalDateTimeValue.localDateTime;
import static org.neo4j.values.storable.LocalDateTimeValue.localDateTimeRaw;
import static org.neo4j.values.storable.LocalTimeValue.localTime;
import static org.neo4j.values.storable.LocalTimeValue.localTimeRaw;
import static org.neo4j.values.storable.TimeValue.time;
import static org.neo4j.values.storable.TimeValue.timeRaw;
import static org.neo4j.values.storable.Values.byteArray;
import static org.neo4j.values.storable.Values.pointValue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;
import org.neo4j.values.utils.TemporalUtil;

public class ValueStream {
    static final byte TINY_STRING = (byte) 0x80;
    static final byte FALSE = (byte) 0x90;
    static final byte TRUE = (byte) 0xA0;
    static final byte INT_8 = (byte) 0xB0;
    static final byte INT_16 = (byte) 0xC0;
    static final byte INT_32 = (byte) 0xC1;
    static final byte INT_64 = (byte) 0xC2;
    static final byte FLOAT_64 = (byte) 0xC3;
    static final byte STRING_8 = (byte) 0xC4;
    static final byte STRING_16 = (byte) 0xC5;
    static final byte STRING_32 = (byte) 0xC6;
    static final byte POINT_2D = (byte) 0xC7;
    static final byte POINT_3D = (byte) 0xC8;
    static final byte DATE = (byte) 0xC9;
    static final byte TIME = (byte) 0xCA;
    static final byte LOCAL_TIME = (byte) 0xCB;
    static final byte LOCAL_DATE_TIME = (byte) 0xCC;
    static final byte DATE_TIME_WITH_ZONE_OFFSET = (byte) 0xCD;
    static final byte DATE_TIME_WITH_ZONE_NAME = (byte) 0xCE;
    static final byte DURATION = (byte) 0xCF;
    static final byte BYTES_8 = (byte) 0xD0;
    static final byte BYTES_16 = (byte) 0xD1;
    static final byte BYTES_32 = (byte) 0xD2;

    static final byte ARRAY_8 = (byte) 0xD3;
    static final byte ARRAY_16 = (byte) 0xD4;
    static final byte ARRAY_32 = (byte) 0xD5;
    static final byte FLOAT_ARRAY = (byte) 0xD6;
    static final byte DOUBLE_ARRAY = (byte) 0xD7;
    static final byte LONG_ARRAY = (byte) 0xD8;
    static final byte INT_ARRAY = (byte) 0xD9;
    static final byte SHORT_ARRAY = (byte) 0xDA;
    static final byte BOOLEAN_ARRAY = (byte) 0xDB;
    static final byte CHAR_ARRAY = (byte) 0xDC;
    static final byte STRING_ARRAY = (byte) 0xDD;
    static final byte DATE_TIME_ARRAY = (byte) 0xDE;
    static final byte LOCAL_DATE_TIME_ARRAY = (byte) 0xDF;
    static final byte DATE_ARRAY = (byte) 0xE0;
    static final byte ZONED_TIME_ARRAY = (byte) 0xE1;
    static final byte TIME_ARRAY = (byte) 0xE2;
    static final byte DURATION_ARRAY = (byte) 0xE3;
    static final byte POINT_ARRAY = (byte) 0xE4;

    static final byte NULL = (byte) 0xE5;

    private static final long PLUS_2_TO_THE_31 = 2147483648L;
    private static final long PLUS_2_TO_THE_15 = 32768L;
    private static final long PLUS_2_TO_THE_7 = 128L;
    private static final long MINUS_2_TO_THE_4 = -16L;
    private static final long MINUS_2_TO_THE_7 = -128L;
    private static final long MINUS_2_TO_THE_15 = -32768L;
    private static final long MINUS_2_TO_THE_31 = -2147483648L;
    private static final char PACKED_CHAR_START_CHAR = (char) 32;
    private static final char PACKED_CHAR_END_CHAR = (char) 126;
    private static final String[] PACKED_CHARS = prePackChars();

    public static void write(WritableChannel out, AnyValue value) throws IOException {
        value.writeTo(new ValueWriter(out));
    }

    public static Value readValue(PeekableChannel in) throws IOException {
        ValueType valType = peekNextType(in);
        return switch (valType) {
            case BOOLEAN -> Values.booleanValue(readBoolean(in));
            case INTEGER -> Values.longValue(readLong(in));
            case FLOAT -> Values.doubleValue(readDouble(in));
            case STRING -> Values.utf8Value(readUTF8(in));
            case POINT_2D -> readPoint2D(in);
            case POINT_3D -> readPoint3D(in);
            case DURATION -> readDuration(in);
            case DATE -> readDate(in);
            case LOCAL_TIME -> readLocalTime(in);
            case TIME -> readTime(in);
            case LOCAL_DATE_TIME -> readLocalDateTime(in);
            case DATE_TIME_WITH_ZONE_OFFSET -> readDateTimeWithZoneOffset(in);
            case DATE_TIME_WITH_ZONE_NAME -> readDateTimeWithZoneName(in);
            case BYTES -> byteArray(readBytes(in, unpackBytesHeader(in)));
            case ARRAY -> readArray(in);
            case NULL -> Values.NO_VALUE;
            default -> throw new IllegalArgumentException("Unknown value type: " + valType);
        };
    }

    private static Value readArray(PeekableChannel in) throws IOException {
        int size = unpackArrayHeader(in);
        byte arrayType = in.get();
        return switch (arrayType) {
            case FLOAT_ARRAY -> Values.floatArray(readFloats(in, size));
            case DOUBLE_ARRAY -> Values.doubleArray(readDoubles(in, size));
            case LONG_ARRAY -> Values.longArray(readLongs(in, size));
            case INT_ARRAY -> Values.intArray(readInts(in, size));
            case SHORT_ARRAY -> Values.shortArray(readShorts(in, size));
            case BOOLEAN_ARRAY -> Values.booleanArray(readBooleans(in, size));
            case CHAR_ARRAY -> Values.charArray(readChars(in, size));
            case STRING_ARRAY -> Values.stringArray(readStrings(in, size));
            case DATE_TIME_ARRAY -> Values.dateTimeArray(readDateTimeWithZoneNames(in, size));
            case LOCAL_DATE_TIME_ARRAY -> Values.localDateTimeArray(readLocalDateTimes(in, size));
            case DATE_ARRAY -> Values.dateArray(readDates(in, size));
            case ZONED_TIME_ARRAY -> Values.localTimeArray(readLocalTimes(in, size));
            case TIME_ARRAY -> Values.timeArray(readTimes(in, size));
            case DURATION_ARRAY -> Values.durationArray(readDurations(in, size));
            case POINT_ARRAY -> Values.pointArray(readPoints(in, size));
            default -> throw new IllegalArgumentException("Unknown array type: " + arrayType);
        };
    }

    private static LocalDate[] readDates(PeekableChannel in, int size) throws IOException {
        LocalDate[] data = new LocalDate[size];
        for (int i = 0; i < size; i++) {
            data[i] = readRawDate(in);
        }
        return data;
    }

    private static LocalTime[] readLocalTimes(PeekableChannel in, int size) throws IOException {
        LocalTime[] data = new LocalTime[size];
        for (int i = 0; i < size; i++) {
            data[i] = readRawLocalTime(in);
        }
        return data;
    }

    private static OffsetTime[] readTimes(PeekableChannel in, int size) throws IOException {
        OffsetTime[] data = new OffsetTime[size];
        for (int i = 0; i < size; i++) {
            data[i] = readRawTime(in);
        }
        return data;
    }

    private static DurationValue[] readDurations(PeekableChannel in, int size) throws IOException {
        DurationValue[] data = new DurationValue[size];
        for (int i = 0; i < size; i++) {
            data[i] = readDuration(in);
        }
        return data;
    }

    private static PointValue[] readPoints(PeekableChannel in, int size) throws IOException {
        PointValue[] data = new PointValue[size];
        for (int i = 0; i < size; i++) {
            data[i] = (PointValue) readValue(in);
        }
        return data;
    }

    private static LocalDateTime[] readLocalDateTimes(PeekableChannel in, int size) throws IOException {
        LocalDateTime[] data = new LocalDateTime[size];
        for (int i = 0; i < size; i++) {
            data[i] = readRawLocalDateTime(in);
        }
        return data;
    }

    private static ZonedDateTime[] readDateTimeWithZoneNames(PeekableChannel in, int size) throws IOException {
        ZonedDateTime[] data = new ZonedDateTime[size];
        for (int i = 0; i < size; i++) {
            byte typeByte = in.get();
            data[i] = switch (typeByte) {
                case DATE_TIME_WITH_ZONE_OFFSET -> readRawDateTimeWithZoneOffset(in);
                case DATE_TIME_WITH_ZONE_NAME -> readRawDateTimeWithZoneName(in);
                default -> throw new IllegalArgumentException("Unknown value type: " + typeByte);
            };
        }
        return data;
    }

    private static String[] readStrings(PeekableChannel in, int size) throws IOException {
        String[] data = new String[size];
        for (int i = 0; i < size; i++) {
            data[i] = readString(in);
        }
        return data;
    }

    private static char[] readChars(PeekableChannel in, int size) throws IOException {
        char[] data = new char[size];
        for (int i = 0; i < size; i++) {
            // TODO: how much do we care about this?
            data[i] = readString(in).charAt(0);
        }
        return data;
    }

    private static boolean[] readBooleans(PeekableChannel in, int size) throws IOException {
        boolean[] data = new boolean[size];
        for (int i = 0; i < size; i++) {
            data[i] = readBoolean(in);
        }
        return data;
    }

    private static float[] readFloats(PeekableChannel in, int size) throws IOException {
        float[] data = new float[size];
        for (int i = 0; i < size; i++) {
            data[i] = (float) readDouble(in);
        }
        return data;
    }

    private static double[] readDoubles(PeekableChannel in, int size) throws IOException {
        double[] data = new double[size];
        for (int i = 0; i < size; i++) {
            data[i] = readDouble(in);
        }
        return data;
    }

    private static long[] readLongs(PeekableChannel in, int size) throws IOException {
        long[] data = new long[size];
        for (int i = 0; i < size; i++) {
            data[i] = readLong(in);
        }
        return data;
    }

    private static int[] readInts(PeekableChannel in, int size) throws IOException {
        int[] data = new int[size];
        for (int i = 0; i < size; i++) {
            data[i] = readInteger(in);
        }
        return data;
    }

    private static short[] readShorts(PeekableChannel in, int size) throws IOException {
        short[] data = new short[size];
        for (int i = 0; i < size; i++) {
            data[i] = (short) readInteger(in);
        }
        return data;
    }

    private static ValueType peekNextType(PeekableChannel in) throws IOException {
        return type(in.peek());
    }

    private static ValueType type(byte markerByte) {
        final byte markerHighNibble = (byte) (markerByte & 0xF0);

        if (markerHighNibble == TINY_STRING) {
            return ValueType.STRING;
        }

        if (markerByte >= MINUS_2_TO_THE_4) {
            return ValueType.INTEGER;
        }

        return switch (markerByte) {
            case TRUE, FALSE -> ValueType.BOOLEAN;
            case INT_8, INT_16, INT_32, INT_64 -> ValueType.INTEGER;
            case FLOAT_64 -> ValueType.FLOAT;
            case TINY_STRING, STRING_8, STRING_16, STRING_32 -> ValueType.STRING;
            case POINT_2D -> ValueType.POINT_2D;
            case POINT_3D -> ValueType.POINT_3D;
            case DATE -> ValueType.DATE;
            case TIME -> ValueType.TIME;
            case LOCAL_TIME -> ValueType.LOCAL_TIME;
            case LOCAL_DATE_TIME -> ValueType.LOCAL_DATE_TIME;
            case DATE_TIME_WITH_ZONE_OFFSET -> ValueType.DATE_TIME_WITH_ZONE_OFFSET;
            case DATE_TIME_WITH_ZONE_NAME -> ValueType.DATE_TIME_WITH_ZONE_NAME;
            case DURATION -> ValueType.DURATION;
            case BYTES_8, BYTES_16, BYTES_32 -> ValueType.BYTES;
            case ARRAY_8, ARRAY_16, ARRAY_32 -> ValueType.ARRAY;
            case NULL -> ValueType.NULL;
            default -> ValueType.RESERVED;
        };
    }

    private static String[] prePackChars() {
        int size = PACKED_CHAR_END_CHAR + 1 - PACKED_CHAR_START_CHAR;
        String[] packedChars = new String[size];
        for (int i = 0; i < size; i++) {
            packedChars[i] = String.valueOf((char) (i + PACKED_CHAR_START_CHAR));
        }
        return packedChars;
    }

    static void write(WritableChannel out, boolean value) throws IOException {
        out.put(value ? TRUE : FALSE);
    }

    public static void writeNoValue(WritableChannel out) throws IOException {
        out.put(NULL);
    }

    static void write(WritableChannel out, long value) throws IOException {
        if (value >= MINUS_2_TO_THE_4 && value < PLUS_2_TO_THE_7) {
            out.put((byte) value);
        } else if (value >= MINUS_2_TO_THE_7 && value < MINUS_2_TO_THE_4) {
            out.put(INT_8).put((byte) value);
        } else if (value >= MINUS_2_TO_THE_15 && value < PLUS_2_TO_THE_15) {
            out.put(INT_16).putShort((short) value);
        } else if (value >= MINUS_2_TO_THE_31 && value < PLUS_2_TO_THE_31) {
            out.put(INT_32).putInt((int) value);
        } else {
            out.put(INT_64).putLong(value);
        }
    }

    static void write(WritableChannel out, double value) throws IOException {
        out.put(FLOAT_64).putDouble(value);
    }

    static void write(WritableChannel out, char character) throws IOException {
        if (character >= PACKED_CHAR_START_CHAR && character <= PACKED_CHAR_END_CHAR) {
            write(out, PACKED_CHARS[character - PACKED_CHAR_START_CHAR]);
        } else {
            write(out, String.valueOf(character));
        }
    }

    static void write(WritableChannel out, byte[] value) throws IOException {
        packBytesHeader(out, value.length);
        out.put(value, 0, value.length);
    }

    static void write(WritableChannel out, String value) throws IOException {
        byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        packStringHeader(out, encoded.length);
        out.put(encoded, encoded.length);
    }

    static void writeUTF8(WritableChannel out, byte[] bytes, int offset, int length) throws IOException {
        packStringHeader(out, length);
        out.put(bytes, offset, length);
    }

    static void packBytesHeader(WritableChannel out, int size) throws IOException {
        packHeader(out, size, BYTES_8, BYTES_16, BYTES_32);
    }

    static void packStringHeader(WritableChannel out, int size) throws IOException {
        packHeader(out, size, TINY_STRING, STRING_8, STRING_16, STRING_32);
    }

    static void packArrayHeader(WritableChannel out, int size) throws IOException {
        packHeader(out, size, ARRAY_8, ARRAY_16, ARRAY_32);
    }

    private static void packHeader(WritableChannel out, int size, byte marker8, byte marker16, byte marker32)
            throws IOException {
        /*
         * The code here is on purpose to test against the maximum value of a signed byte rather than a unsigned byte.
         * We pack values that in range 2^7 ~ 2^8-1 with marker16 instead of marker8
         * to prevent us from breaking any clients that are reading this size as a signed value.
         * Similar case applies to Short.MAX_VALUE
         * */
        if (size <= Byte.MAX_VALUE) {
            out.put(marker8).put((byte) size);
        } else if (size <= Short.MAX_VALUE) {
            out.put(marker16).putShort((short) size);
        } else {
            out.put(marker32).putInt(size);
        }
    }

    private static void packHeader(
            WritableChannel out, int size, byte marker4, byte marker8, byte marker16, byte marker32)
            throws IOException {
        if (size < 0x10) {
            out.put((byte) (marker4 | size));
        } else {
            packHeader(out, size, marker8, marker16, marker32);
        }
    }

    static void writePoint(WritableChannel out, CoordinateReferenceSystem crs, double[] coordinate) throws IOException {
        if (coordinate.length == 2) {
            out.put(POINT_2D);
            write(out, crs.getCode());
            write(out, coordinate[0]);
            write(out, coordinate[1]);
        } else if (coordinate.length == 3) {
            out.put(POINT_3D);
            write(out, crs.getCode());
            write(out, coordinate[0]);
            write(out, coordinate[1]);
            write(out, coordinate[2]);
        } else {
            throw new IllegalArgumentException("Point with 2D or 3D coordinate expected, " + "got crs=" + crs
                    + ", coordinate=" + Arrays.toString(coordinate));
        }
    }

    static void writeDuration(WritableChannel out, long months, long days, long seconds, int nanos) throws IOException {
        out.put(DURATION);
        write(out, months);
        write(out, days);
        write(out, seconds);
        write(out, nanos);
    }

    static void writeDate(WritableChannel out, LocalDate localDate) throws IOException {
        long epochDay = localDate.toEpochDay();

        out.put(DATE);
        write(out, epochDay);
    }

    static void writeLocalTime(WritableChannel out, LocalTime localTime) throws IOException {
        long nanoOfDay = localTime.toNanoOfDay();

        out.put(LOCAL_TIME);
        write(out, nanoOfDay);
    }

    static void writeTime(WritableChannel out, OffsetTime offsetTime) throws IOException {
        long nanosOfDayLocal = offsetTime.toLocalTime().toNanoOfDay();
        int offsetSeconds = offsetTime.getOffset().getTotalSeconds();

        out.put(TIME);
        write(out, nanosOfDayLocal);
        write(out, offsetSeconds);
    }

    static void writeLocalDateTime(WritableChannel out, LocalDateTime localDateTime) throws IOException {
        long epochSecond = localDateTime.toEpochSecond(UTC);
        int nano = localDateTime.getNano();

        out.put(LOCAL_DATE_TIME);
        write(out, epochSecond);
        write(out, nano);
    }

    static void writeDateTime(WritableChannel out, ZonedDateTime zonedDateTime) throws IOException {
        long epochSecondLocal = zonedDateTime.toLocalDateTime().toEpochSecond(UTC);
        int nano = zonedDateTime.getNano();

        ZoneId zone = zonedDateTime.getZone();
        if (zone instanceof ZoneOffset) {
            int offsetSeconds = ((ZoneOffset) zone).getTotalSeconds();

            out.put(DATE_TIME_WITH_ZONE_OFFSET);
            write(out, epochSecondLocal);
            write(out, nano);
            write(out, offsetSeconds);
        } else {
            String zoneId = zone.getId();

            out.put(DATE_TIME_WITH_ZONE_NAME);
            write(out, epochSecondLocal);
            write(out, nano);
            write(out, zoneId);
        }
    }

    private static boolean readBoolean(PeekableChannel in) throws IOException {
        final byte markerByte = in.get();
        return switch (markerByte) {
            case TRUE -> true;
            case FALSE -> false;
            default -> throw new IllegalStateException("Unexpected: " + markerByte);
        };
    }

    private static long readLong(PeekableChannel in) throws IOException {
        final byte markerByte = in.get();
        if (markerByte >= MINUS_2_TO_THE_4) {
            return markerByte;
        }
        return switch (markerByte) {
            case INT_8 -> in.get();
            case INT_16 -> in.getShort();
            case INT_32 -> in.getInt();
            case INT_64 -> in.getLong();
            default -> throw new IllegalStateException("Unexpected: " + markerByte);
        };
    }

    private static byte[] readBytes(PeekableChannel in, int size) throws IOException {
        return readRawBytes(in, size);
    }

    private static String readString(PeekableChannel in) throws IOException {
        return new String(readUTF8(in), StandardCharsets.UTF_8);
    }

    private static int unpackBytesHeader(PeekableChannel in) throws IOException {
        final byte markerByte = in.get();

        int size;
        switch (markerByte) {
            case BYTES_8:
                size = readUINT8(in);
                break;
            case BYTES_16:
                size = readUINT16(in);
                break;
            case BYTES_32: {
                size = readUINT32(in, ValueType.BYTES);
                break;
            }
            default:
                throw new IllegalStateException("Unexpected: " + markerByte);
        }
        return size;
    }

    private static int unpackArrayHeader(PeekableChannel in) throws IOException {
        final byte markerByte = in.get();
        return switch (markerByte) {
            case ARRAY_8 -> readUINT8(in);
            case ARRAY_16 -> readUINT16(in);
            case ARRAY_32 -> readUINT32(in, ValueType.BYTES);
            default -> throw new IllegalStateException("Unexpected: " + markerByte);
        };
    }

    private static int readStringHeader(PeekableChannel in) throws IOException {
        final byte markerByte = in.get();
        final byte markerHighNibble = (byte) (markerByte & 0xF0);
        final byte markerLowNibble = (byte) (markerByte & 0x0F);

        int size;

        if (markerHighNibble == TINY_STRING) {
            size = markerLowNibble;
        } else {
            switch (markerByte) {
                case STRING_8:
                    size = readUINT8(in);
                    break;
                case STRING_16:
                    size = readUINT16(in);
                    break;
                case STRING_32: {
                    size = readUINT32(in, ValueType.STRING);
                    break;
                }
                default:
                    throw new IllegalStateException("Unexpected: " + markerByte);
            }
        }

        return size;
    }

    private static byte[] readUTF8(PeekableChannel in) throws IOException {
        int size = readStringHeader(in);
        return readRawBytes(in, size);
    }

    private static int readUINT8(PeekableChannel in) throws IOException {
        return in.get() & 0xFF;
    }

    private static int readUINT16(PeekableChannel in) throws IOException {
        return in.getShort() & 0xFFFF;
    }

    private static long readUINT32(PeekableChannel in) throws IOException {
        return in.getInt() & 0xFFFFFFFFL;
    }

    private static int readUINT32(PeekableChannel in, ValueType type) throws IOException {
        long longSize = readUINT32(in);
        if (longSize <= Integer.MAX_VALUE) {
            return (int) longSize;
        } else {
            throw new IllegalStateException(String.format("%s_32 too long for Java", type));
        }
    }

    private static byte[] readRawBytes(PeekableChannel in, int size) throws IOException {
        if (size == 0) {
            return new byte[0];
        } else {
            byte[] heapBuffer = new byte[size];
            in.get(heapBuffer, heapBuffer.length);
            return heapBuffer;
        }
    }

    private static double readDouble(PeekableChannel in) throws IOException {
        final byte markerByte = in.get();
        if (markerByte == FLOAT_64) {
            return in.getDouble();
        }
        throw new IllegalStateException("Unexpected: " + markerByte);
    }

    private static int readInteger(PeekableChannel in) throws IOException {
        final byte markerByte = in.get();
        if (markerByte >= MINUS_2_TO_THE_4) {
            return markerByte;
        }
        return switch (markerByte) {
            case INT_8 -> in.get();
            case INT_16 -> in.getShort();
            case INT_32 -> in.getInt();
            case INT_64 -> throw new IllegalStateException(
                    "Unexpectedly large Integer value unpacked (" + in.getLong() + ")");
            default -> throw new IllegalStateException("Unexpected: " + markerByte);
        };
    }

    private static PointValue readPoint2D(PeekableChannel in) throws IOException {
        in.get();
        int crsCode = readInteger(in);
        CoordinateReferenceSystem crs = CoordinateReferenceSystem.get(crsCode);
        double[] coordinates = {readDouble(in), readDouble(in)};
        return pointValue(crs, coordinates);
    }

    private static PointValue readPoint3D(PeekableChannel in) throws IOException {
        in.get();
        int crsCode = readInteger(in);
        CoordinateReferenceSystem crs = CoordinateReferenceSystem.get(crsCode);
        double[] coordinates = {readDouble(in), readDouble(in), readDouble(in)};
        return pointValue(crs, coordinates);
    }

    private static DurationValue readDuration(PeekableChannel in) throws IOException {
        in.get();
        long months = readLong(in);
        long days = readLong(in);
        long seconds = readLong(in);
        long nanos = readInteger(in);
        return duration(months, days, seconds, nanos);
    }

    private static DateValue readDate(PeekableChannel in) throws IOException {
        return date(readRawDate(in));
    }

    private static LocalDate readRawDate(PeekableChannel in) throws IOException {
        in.get();
        long epochDay = readLong(in);
        return epochDateRaw(epochDay);
    }

    private static LocalTimeValue readLocalTime(PeekableChannel in) throws IOException {
        return localTime(readRawLocalTime(in));
    }

    private static LocalTime readRawLocalTime(PeekableChannel in) throws IOException {
        in.get();
        long nanoOfDay = readLong(in);
        return localTimeRaw(nanoOfDay);
    }

    private static TimeValue readTime(PeekableChannel in) throws IOException {
        return time(readRawTime(in));
    }

    private static OffsetTime readRawTime(PeekableChannel in) throws IOException {
        in.get();
        long nanosOfDayLocal = readLong(in);
        int offsetSeconds = readInteger(in);
        return timeRaw(
                TemporalUtil.nanosOfDayToUTC(nanosOfDayLocal, offsetSeconds), ZoneOffset.ofTotalSeconds(offsetSeconds));
    }

    private static LocalDateTime readRawLocalDateTime(PeekableChannel in) throws IOException {
        in.get();
        long epochSecond = readLong(in);
        long nano = readLong(in);
        return localDateTimeRaw(epochSecond, nano);
    }

    private static LocalDateTimeValue readLocalDateTime(PeekableChannel in) throws IOException {
        return localDateTime(readRawLocalDateTime(in));
    }

    private static DateTimeValue readDateTimeWithZoneOffset(PeekableChannel in) throws IOException {
        in.get();
        long epochSecondLocal = readLong(in);
        long nano = readLong(in);
        int offsetSeconds = readInteger(in);
        return datetime(newZonedDateTime(epochSecondLocal, nano, ZoneOffset.ofTotalSeconds(offsetSeconds)));
    }

    private static ZonedDateTime readRawDateTimeWithZoneOffset(PeekableChannel in) throws IOException {
        long epochSecondLocal = readLong(in);
        long nano = readLong(in);
        int offsetSeconds = readInteger(in);
        return newZonedDateTime(epochSecondLocal, nano, ZoneOffset.ofTotalSeconds(offsetSeconds));
    }

    private static DateTimeValue readDateTimeWithZoneName(PeekableChannel in) throws IOException {
        in.get();
        long epochSecondLocal = readLong(in);
        long nano = readLong(in);
        String zoneId = readString(in);
        return datetime(newZonedDateTime(epochSecondLocal, nano, ZoneId.of(zoneId)));
    }

    private static ZonedDateTime readRawDateTimeWithZoneName(PeekableChannel in) throws IOException {
        long epochSecondLocal = readLong(in);
        long nano = readLong(in);
        String zoneId = readString(in);
        return newZonedDateTime(epochSecondLocal, nano, ZoneId.of(zoneId));
    }

    private static ZonedDateTime newZonedDateTime(long epochSecondLocal, long nano, ZoneId zoneId) {
        Instant instant = Instant.ofEpochSecond(epochSecondLocal, nano);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, UTC);
        return ZonedDateTime.of(localDateTime, zoneId);
    }
}
