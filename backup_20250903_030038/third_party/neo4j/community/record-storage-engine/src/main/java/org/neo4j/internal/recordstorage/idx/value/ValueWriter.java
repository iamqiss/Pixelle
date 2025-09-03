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

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.List;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.values.AnyValueWriter;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.TextArray;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.RelationshipValue;
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualRelationshipValue;

class ValueWriter implements AnyValueWriter<IOException> {
    private final WritableChannel out;

    ValueWriter(WritableChannel out) {
        this.out = out;
    }

    @Override
    public EntityMode entityMode() {
        return EntityMode.FULL;
    }

    @Override
    public void writeNodeReference(long nodeId) {
        throw new UnsupportedOperationException("Cannot write entities");
    }

    @Override
    public void writeNode(String elementId, long nodeId, TextArray labels, MapValue properties, boolean isDeleted) {
        throw new UnsupportedOperationException("Cannot write entities");
    }

    @Override
    public void writeRelationshipReference(long relationshipId) {
        throw new UnsupportedOperationException("Cannot write entities");
    }

    @Override
    public void writeRelationship(
            String elementId,
            long relId,
            String startNodeElementId,
            long startNodeId,
            String endNodeElementId,
            long endNodeId,
            TextValue type,
            MapValue properties,
            boolean isDeleted) {
        throw new UnsupportedOperationException("Cannot write entities");
    }

    @Override
    public void beginMap(int size) {
        throw new UnsupportedOperationException("Cannot write virtual types");
    }

    @Override
    public void endMap() {
        // do nothing
    }

    @Override
    public void beginList(int size) {
        throw new UnsupportedOperationException("Cannot write virtual types");
    }

    @Override
    public void endList() {
        // do nothing
    }

    @Override
    public void writePathReference(long[] nodes, long[] relationships) {
        throw new UnsupportedOperationException("Cannot write virtual types");
    }

    @Override
    public void writePathReference(VirtualNodeValue[] nodes, VirtualRelationshipValue[] relationships) {
        throw new UnsupportedOperationException("Cannot write virtual types");
    }

    @Override
    public void writePathReference(List<VirtualNodeValue> nodes, List<VirtualRelationshipValue> relationships) {
        throw new UnsupportedOperationException("Cannot write virtual types");
    }

    @Override
    public void writePath(NodeValue[] nodes, RelationshipValue[] relationships) {
        throw new UnsupportedOperationException("Cannot write paths");
    }

    @Override
    public void writeVirtualNodeHack(Object node) {
        throw new UnsupportedOperationException("Cannot write virtual types");
    }

    @Override
    public void writeVirtualRelationshipHack(Object relationship) {
        throw new UnsupportedOperationException("Cannot write virtual types");
    }

    @Override
    public void writePoint(CoordinateReferenceSystem crs, double[] coordinate) throws IOException {
        ValueStream.writePoint(out, crs, coordinate);
    }

    @Override
    public void writeDuration(long months, long days, long seconds, int nanos) throws IOException {
        ValueStream.writeDuration(out, months, days, seconds, nanos);
    }

    @Override
    public void writeDate(LocalDate localDate) throws IOException {
        ValueStream.writeDate(out, localDate);
    }

    @Override
    public void writeLocalTime(LocalTime localTime) throws IOException {
        ValueStream.writeLocalTime(out, localTime);
    }

    @Override
    public void writeTime(OffsetTime offsetTime) throws IOException {
        ValueStream.writeTime(out, offsetTime);
    }

    @Override
    public void writeLocalDateTime(LocalDateTime localDateTime) throws IOException {
        ValueStream.writeLocalDateTime(out, localDateTime);
    }

    @Override
    public void writeDateTime(ZonedDateTime zonedDateTime) throws IOException {
        ValueStream.writeDateTime(out, zonedDateTime);
    }

    @Override
    public void writeNull() throws IOException {
        ValueStream.writeNoValue(out);
    }

    @Override
    public void writeBoolean(boolean value) throws IOException {
        ValueStream.write(out, value);
    }

    @Override
    public void writeInteger(byte value) throws IOException {
        ValueStream.write(out, value);
    }

    @Override
    public void writeInteger(short value) throws IOException {
        ValueStream.write(out, value);
    }

    @Override
    public void writeInteger(int value) throws IOException {
        ValueStream.write(out, value);
    }

    @Override
    public void writeInteger(long value) throws IOException {
        ValueStream.write(out, value);
    }

    @Override
    public void writeFloatingPoint(float value) throws IOException {
        ValueStream.write(out, value);
    }

    @Override
    public void writeFloatingPoint(double value) throws IOException {
        ValueStream.write(out, value);
    }

    @Override
    public void writeUTF8(byte[] bytes, int offset, int length) throws IOException {
        ValueStream.writeUTF8(out, bytes, offset, length);
    }

    @Override
    public void writeString(String value) throws IOException {
        ValueStream.write(out, value);
    }

    @Override
    public void writeString(char value) throws IOException {
        ValueStream.write(out, value);
    }

    @Override
    public void beginArray(int size, ArrayType arrayType) throws IOException {
        ValueStream.packArrayHeader(out, size);
        byte type =
                switch (arrayType) {
                    case SHORT -> ValueStream.SHORT_ARRAY;
                    case INT -> ValueStream.INT_ARRAY;
                    case LONG -> ValueStream.LONG_ARRAY;
                    case FLOAT -> ValueStream.FLOAT_ARRAY;
                    case DOUBLE -> ValueStream.DOUBLE_ARRAY;
                    case BOOLEAN -> ValueStream.BOOLEAN_ARRAY;
                    case STRING -> ValueStream.STRING_ARRAY;
                    case CHAR -> ValueStream.CHAR_ARRAY;
                    case POINT -> ValueStream.POINT_ARRAY;

                    case ZONED_DATE_TIME -> ValueStream.DATE_TIME_ARRAY;
                    case LOCAL_DATE_TIME -> ValueStream.LOCAL_DATE_TIME_ARRAY;
                    case DATE -> ValueStream.DATE_ARRAY;
                    case ZONED_TIME -> ValueStream.TIME_ARRAY;
                    case LOCAL_TIME -> ValueStream.ZONED_TIME_ARRAY;
                    case DURATION -> ValueStream.DURATION_ARRAY;
                    default -> throw new IllegalStateException("Unexpected value: " + arrayType);
                };
        out.put(type);
    }

    @Override
    public void endArray() {
        // Do nothing
    }

    @Override
    public void writeByteArray(byte[] value) throws IOException {
        ValueStream.write(out, value);
    }
}
