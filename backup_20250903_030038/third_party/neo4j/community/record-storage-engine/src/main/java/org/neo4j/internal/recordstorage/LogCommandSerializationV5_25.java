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
package org.neo4j.internal.recordstorage;

import static org.neo4j.internal.recordstorage.idx.PositiveNumberEncoder.readNumber;
import static org.neo4j.internal.recordstorage.idx.PositiveNumberEncoder.writeNumber;

import java.io.IOException;
import org.neo4j.internal.recordstorage.idx.value.PeekableChannel;
import org.neo4j.internal.recordstorage.idx.value.ValueStream;
import org.neo4j.internal.recordstorage.indexcommand.IndexUpdateCommand;
import org.neo4j.internal.recordstorage.indexcommand.TokenIndexUpdateCommand;
import org.neo4j.internal.recordstorage.indexcommand.ValueIndexUpdateCommand;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.storageengine.api.UpdateMode;
import org.neo4j.values.storable.Value;

class LogCommandSerializationV5_25 extends LogCommandSerializationV5_23 {
    static final LogCommandSerializationV5_25 INSTANCE = new LogCommandSerializationV5_25();

    @Override
    protected Command readIndexUpdateCommand(ReadableChannel channel) throws IOException {
        return read(this, channel);
    }

    @Override
    public void writeIndexUpdateCommand(WritableChannel channel, IndexUpdateCommand command) throws IOException {
        channel.put(NeoCommandType.INDEX_UPDATE_COMMAND);
        write(channel, command);
    }

    @Override
    public KernelVersion kernelVersion() {
        return KernelVersion.V5_25;
    }

    static void write(WritableChannel out, IndexUpdateCommand command) throws IOException {
        boolean tokenIndex = isTokenIndex(command);
        writeHeader(out, command, tokenIndex);
        writeNumber(out, command.getIndexId());
        writeNumber(out, command.getEntityId());
        if (tokenIndex) {
            writeTokenPart(out, (TokenIndexUpdateCommand) command);
        } else {
            writeValuePart(out, (ValueIndexUpdateCommand) command);
        }
    }

    static IndexUpdateCommand read(LogCommandSerialization serialization, ReadableChannel in) throws IOException {
        byte header = in.get();
        boolean tokenIndex = isTokenIndex(header);
        UpdateMode updateMode = readUpdateMode(header);
        long indexId = readNumber(in);
        long entityId = readNumber(in);

        if (tokenIndex) {
            return readTokenPart(serialization, in, updateMode, indexId, entityId);
        } else {
            return readValuePart(serialization, in, updateMode, indexId, entityId);
        }
    }

    private static boolean isTokenIndex(byte header) {
        return (header & (1 << 7)) == 0;
    }

    private static UpdateMode readUpdateMode(byte header) {
        int modeOrdinal = (header >> 5) & 3;
        return switch (modeOrdinal) {
            case 0 -> UpdateMode.ADDED;
            case 1 -> UpdateMode.CHANGED;
            case 2 -> UpdateMode.REMOVED;
            default -> throw new IllegalArgumentException("Weird header: " + header);
        };
    }

    private static void writeValuePart(WritableChannel out, ValueIndexUpdateCommand command) throws IOException {
        switch (command.getUpdateMode()) {
            case ADDED:
            case REMOVED:
                writeValueArray(out, command.getAfter());
                break;
            case CHANGED:
                writeValueArray(out, command.getBefore());
                writeValueArray(out, command.getAfter());
                break;
            default:
                throw new IllegalArgumentException();
        }
    }

    private static ValueIndexUpdateCommand readValuePart(
            LogCommandSerialization serialization,
            ReadableChannel in,
            UpdateMode updateMode,
            long indexId,
            long entityId)
            throws IOException {
        switch (updateMode) {
            case ADDED:
            case REMOVED: {
                Value[] values = readValueArray(in);
                return new ValueIndexUpdateCommand(serialization, updateMode, indexId, entityId, null, values);
            }
            case CHANGED: {
                Value[] before = readValueArray(in);
                Value[] values = readValueArray(in);
                return new ValueIndexUpdateCommand(serialization, updateMode, indexId, entityId, before, values);
            }
            default:
                throw new IllegalArgumentException();
        }
    }

    private static void writeValueArray(WritableChannel out, Value[] values) throws IOException {
        writeNumber(out, values.length);
        for (Value value : values) {
            ValueStream.write(out, value);
        }
    }

    private static Value[] readValueArray(ReadableChannel in) throws IOException {
        int length = (int) readNumber(in);
        Value[] values = new Value[length];

        var peekableChannel = new PeekableChannel(in);
        for (int i = 0; i < length; i++) {
            values[i] = ValueStream.readValue(peekableChannel);
        }

        return values;
    }

    private static void writeTokenPart(WritableChannel out, TokenIndexUpdateCommand command) throws IOException {
        switch (command.getUpdateMode()) {
            case ADDED:
            case REMOVED:
                writeTokenArray(out, command.getAfter());
                break;
            case CHANGED:
                writeTokenArray(out, command.getBefore());
                writeTokenArray(out, command.getAfter());
                break;
            default:
                throw new IllegalArgumentException();
        }
    }

    private static TokenIndexUpdateCommand readTokenPart(
            LogCommandSerialization serialization,
            ReadableChannel in,
            UpdateMode updateMode,
            long indexId,
            long entityId)
            throws IOException {
        switch (updateMode) {
            case ADDED:
            case REMOVED: {
                int[] values = readTokenArray(in);
                return new TokenIndexUpdateCommand(serialization, updateMode, indexId, entityId, null, values);
            }
            case CHANGED: {
                int[] before = readTokenArray(in);
                int[] values = readTokenArray(in);
                return new TokenIndexUpdateCommand(serialization, updateMode, indexId, entityId, before, values);
            }
            default:
                throw new IllegalArgumentException();
        }
    }

    private static int[] readTokenArray(ReadableChannel in) throws IOException {
        int length = (int) readNumber(in);
        int[] tokens = new int[length];

        for (int i = 0; i < length; i++) {
            tokens[i] = (int) readNumber(in);
        }

        return tokens;
    }

    private static void writeTokenArray(WritableChannel out, int[] tokens) throws IOException {
        writeNumber(out, tokens.length);
        for (int token : tokens) {
            writeNumber(out, token);
        }
    }

    private static void writeHeader(WritableChannel out, IndexUpdateCommand command, boolean tokenIndex)
            throws IOException {
        byte header = 0;
        if (!tokenIndex) {
            header = (byte) (1 << 7);
        }

        header |= (byte) (command.getUpdateMode().ordinal() << 5);
        out.put(header);
    }

    private static boolean isTokenIndex(IndexUpdateCommand command) {
        return command instanceof TokenIndexUpdateCommand;
    }
}
