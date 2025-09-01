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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.internal.recordstorage.LogCommandSerializationV5_25.read;
import static org.neo4j.internal.recordstorage.LogCommandSerializationV5_25.write;
import static org.neo4j.test.LatestVersions.LATEST_KERNEL_VERSION;
import static org.neo4j.values.storable.Values.longValue;

import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.recordstorage.indexcommand.IndexUpdateCommand;
import org.neo4j.internal.recordstorage.indexcommand.TokenIndexUpdateCommand;
import org.neo4j.internal.recordstorage.indexcommand.ValueIndexUpdateCommand;
import org.neo4j.kernel.impl.transaction.log.InMemoryClosableChannel;
import org.neo4j.storageengine.api.UpdateMode;
import org.neo4j.values.storable.Value;

class IndexUpdateCommandEncoderTest {
    private final InMemoryClosableChannel channel = new InMemoryClosableChannel();
    private final LogCommandSerialization serialization =
            RecordStorageCommandReaderFactory.INSTANCE.get(LATEST_KERNEL_VERSION);

    @Test
    void packTokensAdd() throws IOException {
        var command = new TokenIndexUpdateCommand(
                serialization, UpdateMode.ADDED, 123, 3456789, null, new int[] {1, 22, 333});
        write(channel, command);
        var readCommand = read(serialization, channel);
        assertCommandsEqual(command, readCommand);
    }

    @Test
    void packTokensChange() throws IOException {
        var command = new TokenIndexUpdateCommand(
                RecordStorageCommandReaderFactory.INSTANCE.get(LATEST_KERNEL_VERSION),
                UpdateMode.CHANGED,
                123,
                3456789,
                new int[] {12345},
                new int[] {1, 22, 333});
        write(channel, command);
        var readCommand = read(serialization, channel);
        assertCommandsEqual(command, readCommand);
    }

    @Test
    void packTokensRemove() throws IOException {
        var command = new TokenIndexUpdateCommand(
                RecordStorageCommandReaderFactory.INSTANCE.get(LATEST_KERNEL_VERSION),
                UpdateMode.REMOVED,
                123,
                3456789,
                null,
                new int[] {1, 22, 333});
        write(channel, command);
        var readCommand = read(serialization, channel);
        assertCommandsEqual(command, readCommand);
    }

    @Test
    void packValuesAdd() throws IOException {
        var command = new ValueIndexUpdateCommand(
                RecordStorageCommandReaderFactory.INSTANCE.get(LATEST_KERNEL_VERSION),
                UpdateMode.ADDED,
                123,
                3456789,
                null,
                new Value[] {longValue(1), longValue(22), longValue(333)});
        write(channel, command);
        var readCommand = read(serialization, channel);
        assertCommandsEqual(command, readCommand);
    }

    @Test
    void packValuesChange() throws IOException {
        var command = new ValueIndexUpdateCommand(
                RecordStorageCommandReaderFactory.INSTANCE.get(LATEST_KERNEL_VERSION),
                UpdateMode.CHANGED,
                123,
                3456789,
                new Value[] {longValue(12345)},
                new Value[] {longValue(1), longValue(22), longValue(333)});
        write(channel, command);
        var readCommand = read(serialization, channel);
        assertCommandsEqual(command, readCommand);
    }

    @Test
    void packValuesRemove() throws IOException {
        var command = new ValueIndexUpdateCommand(
                RecordStorageCommandReaderFactory.INSTANCE.get(LATEST_KERNEL_VERSION),
                UpdateMode.REMOVED,
                123,
                3456789,
                null,
                new Value[] {longValue(1), longValue(22), longValue(333)});
        write(channel, command);
        var readCommand = read(serialization, channel);
        assertCommandsEqual(command, readCommand);
    }

    private void assertCommandsEqual(IndexUpdateCommand expected, IndexUpdateCommand read) {
        assertEquals(expected.getClass(), read.getClass());
        assertEquals(expected.getUpdateMode(), read.getUpdateMode());
        assertEquals(expected.getIndexId(), read.getIndexId());
        assertEquals(expected.getEntityId(), read.getEntityId());

        if (expected instanceof TokenIndexUpdateCommand expectedCast) {
            TokenIndexUpdateCommand readCast = (TokenIndexUpdateCommand) read;
            assertArrayEquals(expectedCast.getBefore(), readCast.getBefore());
            assertArrayEquals(expectedCast.getAfter(), readCast.getAfter());
        } else {
            ValueIndexUpdateCommand expectedCast = (ValueIndexUpdateCommand) expected;
            ValueIndexUpdateCommand readCast = (ValueIndexUpdateCommand) read;
            assertArrayEquals(expectedCast.getBefore(), readCast.getBefore());
            assertArrayEquals(expectedCast.getAfter(), readCast.getAfter());
        }
    }
}
