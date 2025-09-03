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
package org.neo4j.internal.recordstorage.indexcommand;

import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;

import java.io.IOException;
import java.util.Arrays;
import org.neo4j.internal.recordstorage.CommandVisitor;
import org.neo4j.internal.recordstorage.LogCommandSerialization;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.storageengine.api.UpdateMode;
import org.neo4j.string.Mask;

public class TokenIndexUpdateCommand extends IndexUpdateCommand<int[]> {
    static final long SHALLOW_SIZE = shallowSizeOfInstance(TokenIndexUpdateCommand.class);

    private final int[] before;
    private final int[] values;

    public TokenIndexUpdateCommand(
            LogCommandSerialization serialization,
            UpdateMode updateMode,
            long indexId,
            long entityId,
            int[] before,
            int[] values) {
        super(serialization, updateMode, indexId, entityId);
        this.before = before;
        this.values = values;
    }

    @Override
    public int[] getBefore() {
        return before;
    }

    @Override
    public int[] getAfter() {
        return values;
    }

    @Override
    public String toString(Mask mask) {
        return String.format(
                "TokenIndexUpdateCommand[mode:%s, indexId:%d, entityId:%d, before:%s, after:%s]",
                updateMode, indexId, entityId, Arrays.toString(before), Arrays.toString(values));
    }

    @Override
    public boolean handle(CommandVisitor handler) throws IOException {
        return handler.visitIndexUpdateCommand(this);
    }

    @Override
    public void serialize(WritableChannel channel) throws IOException {
        serialization.writeIndexUpdateCommand(channel, this);
    }
}
