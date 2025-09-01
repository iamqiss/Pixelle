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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.neo4j.collection.trackable.HeapTrackingCollections;
import org.neo4j.internal.recordstorage.LogCommandSerialization;
import org.neo4j.internal.recordstorage.RecordState;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.TokenIndexEntryUpdate;
import org.neo4j.storageengine.api.UpdateMode;
import org.neo4j.storageengine.api.ValueIndexEntryUpdate;
import org.neo4j.values.storable.Value;

public class IndexRecordState implements RecordState {

    private final LogCommandSerialization serialization;
    private final List<TokenIndexEntryUpdate<IndexDescriptor>> tokenIndexUpdates = new ArrayList<>();
    private final Map<IndexEntityPair, ValueIndexEntryUpdate<IndexDescriptor>> valueIndexUpdates = new HashMap<>();

    public IndexRecordState(LogCommandSerialization serialization) {
        this.serialization = serialization;
    }

    @Override
    public void extractCommands(Collection<StorageCommand> target, MemoryTracker memoryTracker) {
        int valueIndexCommandsSize = valueIndexUpdates.size();
        int tokenIndexCommandsSize = tokenIndexUpdates.size();
        if (valueIndexCommandsSize + tokenIndexCommandsSize == 0) {
            return;
        }
        memoryTracker.allocateHeap(valueIndexCommandsSize * ValueIndexUpdateCommand.SHALLOW_SIZE);
        memoryTracker.allocateHeap(tokenIndexCommandsSize * TokenIndexUpdateCommand.SHALLOW_SIZE);
        extractValueCommands(target, memoryTracker);
        extractTokenCommands(target, memoryTracker);
    }

    private void extractTokenCommands(Collection<StorageCommand> target, MemoryTracker memoryTracker) {
        try (var tokenIndexIndexCommands =
                HeapTrackingCollections.<TokenIndexUpdateCommand>newArrayList(memoryTracker)) {
            for (TokenIndexEntryUpdate<IndexDescriptor> update : tokenIndexUpdates) {
                tokenIndexIndexCommands.add(new TokenIndexUpdateCommand(
                        serialization,
                        update.updateMode(),
                        update.indexKey().getId(),
                        update.getEntityId(),
                        update.beforeValues(),
                        update.values()));
            }

            tokenIndexIndexCommands.sort(IndexComamdComparator.INDEX_COMMANDS_COMPARATOR);
            target.addAll(tokenIndexIndexCommands);
        }
    }

    private void extractValueCommands(Collection<StorageCommand> target, MemoryTracker memoryTracker) {
        try (var valueIndexCommands = HeapTrackingCollections.<ValueIndexUpdateCommand>newArrayList(memoryTracker)) {
            for (ValueIndexEntryUpdate<IndexDescriptor> update : valueIndexUpdates.values()) {
                Value[] before = null;
                if (update.updateMode() == UpdateMode.CHANGED) {
                    before = update.beforeValues();
                }

                valueIndexCommands.add(new ValueIndexUpdateCommand(
                        serialization,
                        update.updateMode(),
                        update.indexKey().getId(),
                        update.getEntityId(),
                        before,
                        update.values()));
            }
            valueIndexCommands.sort(IndexComamdComparator.INDEX_COMMANDS_COMPARATOR);
            target.addAll(valueIndexCommands);
        }
    }

    public void addTokenUpdate(TokenIndexEntryUpdate<IndexDescriptor> tokenIndexUpdate) {
        tokenIndexUpdates.add(tokenIndexUpdate);
    }

    public void putValueUpdate(IndexEntityPair key, ValueIndexEntryUpdate<IndexDescriptor> update) {
        valueIndexUpdates.put(key, update);
    }

    public ValueIndexEntryUpdate<IndexDescriptor> getValueUpdate(IndexEntityPair key) {
        return valueIndexUpdates.get(key);
    }

    private static class IndexComamdComparator implements Comparator<IndexUpdateCommand> {

        static final IndexComamdComparator INDEX_COMMANDS_COMPARATOR = new IndexComamdComparator();

        private IndexComamdComparator() {}

        @Override
        public int compare(IndexUpdateCommand o1, IndexUpdateCommand o2) {
            int result = Long.compare(o1.getIndexId(), o2.getIndexId());
            if (result != 0) {
                return result;
            }
            return Long.compare(o1.getEntityId(), o2.getEntityId());
        }
    }

    record IndexEntityPair(long indexId, long entityId) {}
}
