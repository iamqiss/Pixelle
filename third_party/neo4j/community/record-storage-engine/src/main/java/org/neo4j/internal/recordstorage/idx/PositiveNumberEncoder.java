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

import java.io.IOException;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;

/**
 * An encoder and decoder of positive numbers less than
 * 0xF800 0000 0000 0000.
 * It compresses numbers less than 0x08 0000 0000
 * which is ideal for serialising entity IDs, index IDs,
 * labels and relationship types.
 */
public class PositiveNumberEncoder {

    private static final byte MARKER_1 = (byte) 0x80;
    private static final byte MARKER_2 = (byte) 0x40;
    private static final byte MARKER_3 = (byte) 0x20;
    private static final byte MARKER_4 = (byte) 0x10;
    private static final byte MARKER_5 = (byte) 0x08;

    public static void writeNumber(WritableChannel out, long number) throws IOException {
        if ((number & 0xF800_0000_0000_0000L) != 0) {
            throw new IllegalArgumentException("The submitted number is not supported: " + number);
        }

        if ((number & 0xFFFF_FFFF_FFFF_FF80L) == 0) {
            out.put((byte) (number | MARKER_1));
            return;
        }

        if ((number & 0xFFFF_FFFF_FFFF_C000L) == 0) {
            out.put((byte) ((number >> 8) | MARKER_2));
            out.put((byte) number);
            return;
        }
        if ((number & 0xFFFF_FFFF_FFE0_0000L) == 0) {
            out.put((byte) ((number >> 16) | MARKER_3));
            out.put((byte) (number >> 8));
            out.put((byte) number);
            return;
        }

        if ((number & 0xFFFF_FFFF_F000_0000L) == 0) {
            out.put((byte) ((number >> 24) | MARKER_4));
            out.put((byte) (number >> 16));
            out.put((byte) (number >> 8));
            out.put((byte) number);
            return;
        }

        if ((number & 0xFFFF_FFF8_0000_0000L) == 0) {
            out.put((byte) ((number >> 32) | MARKER_5));
            out.put((byte) (number >> 24));
            out.put((byte) (number >> 16));
            out.put((byte) (number >> 8));
            out.put((byte) number);
            return;
        }

        out.put((byte) (number >> 56));
        out.put((byte) (number >> 48));
        out.put((byte) (number >> 40));
        out.put((byte) (number >> 32));
        out.put((byte) (number >> 24));
        out.put((byte) (number >> 16));
        out.put((byte) (number >> 8));
        out.put((byte) number);
    }

    public static long readNumber(ReadableChannel in) throws IOException {
        byte firstByte = in.get();
        if ((firstByte & MARKER_1) != 0) {
            return firstByte ^ MARKER_1;
        }

        if ((firstByte & MARKER_2) != 0) {
            return ((firstByte ^ MARKER_2) & 0xff) << 8 | ((int) in.get() & 0xff);
        }

        if ((firstByte & MARKER_3) != 0) {
            return ((firstByte ^ MARKER_3) & 0xff) << 16 | ((int) in.get() & 0xff) << 8 | ((int) in.get() & 0xff);
        }

        if ((firstByte & MARKER_4) != 0) {
            return ((firstByte ^ MARKER_4) & 0xFFL) << 24
                    | ((int) in.get() & 0xff) << 16
                    | ((int) in.get() & 0xff) << 8
                    | ((int) in.get() & 0xff);
        }

        if ((firstByte & MARKER_5) != 0) {
            return ((firstByte ^ MARKER_5) & 0xFFL) << 32
                    | ((int) in.get() & 0xFFL) << 24
                    | ((int) in.get() & 0xFF) << 16
                    | ((int) in.get() & 0xFF) << 8
                    | ((int) in.get() & 0xFF);
        }

        return (firstByte & 0xFFL) << 56
                | (in.get() & 0xFFL) << 48
                | (in.get() & 0xFFL) << 40
                | (in.get() & 0xFFL) << 32
                | (in.get() & 0xFFL) << 24
                | ((int) in.get() & 0xff) << 16
                | ((int) in.get() & 0xff) << 8
                | ((int) in.get() & 0xff);
    }
}
