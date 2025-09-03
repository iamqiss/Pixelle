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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.neo4j.common.Subject;
import org.neo4j.internal.recordstorage.indexcommand.IndexUpdateCommand;
import org.neo4j.internal.recordstorage.indexcommand.TokenIndexUpdateCommand;
import org.neo4j.internal.recordstorage.indexcommand.ValueIndexUpdateCommand;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaCache;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.IndexUpdateListener;
import org.neo4j.storageengine.api.StorageEngineTransaction;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.storageengine.util.IndexUpdatesWorkSync;

public class IndexCommandTransactionApplierFactory implements TransactionApplierFactory {
    private final IndexUpdateListener indexUpdateListener;
    private final SchemaCache schemaCache;
    private final TransactionApplicationMode mode;
    private final IndexUpdatesWorkSync indexUpdatesSync;

    public IndexCommandTransactionApplierFactory(
            IndexUpdateListener indexUpdateListener,
            IndexUpdatesWorkSync indexUpdatesSync,
            SchemaCache schemaCache,
            TransactionApplicationMode mode) {
        this.indexUpdateListener = indexUpdateListener;
        this.indexUpdatesSync = indexUpdatesSync;
        this.schemaCache = schemaCache;
        this.mode = mode;
    }

    @Override
    public TransactionApplier startTx(StorageEngineTransaction transaction, BatchContext batchContext) {
        var commandSelector = mode.isReverseStep() ? CommandSelector.REVERSE : CommandSelector.NORMAL;
        return new SingleTransactionApplier(transaction, batchContext, transaction.cursorContext(), commandSelector);
    }

    /**
     * Made as an internal non-static class here since the batch applier has so much interaction with
     * the transaction applier such that keeping them apart would incur too much data structures and interfaces
     * purely for communicating between the two to make the code hard to read.
     */
    private class SingleTransactionApplier extends TransactionApplier.Adapter {
        private final Subject subject;
        private List<IndexDescriptor> createdIndexes;
        private final IndexActivator indexActivator;
        private final CursorContext cursorContext;
        private final CommandSelector commandSelector;
        private final List<IndexEntryUpdate<IndexDescriptor>> indexUpdates = new ArrayList<>();

        SingleTransactionApplier(
                StorageEngineTransaction transaction,
                BatchContext batchContext,
                CursorContext cursorContext,
                CommandSelector commandSelector) {
            this.subject = transaction.subject();
            this.indexActivator = batchContext.getIndexActivator();
            this.cursorContext = cursorContext;
            this.commandSelector = commandSelector;
        }

        @Override
        public void close() throws IOException {
            if (!indexUpdates.isEmpty()) {
                try (var indexUpdatesBatch = indexUpdatesSync.newBatch(cursorContext)) {
                    indexUpdatesBatch.indexUpdates(indexUpdates);
                    indexUpdatesBatch.applyAsync();
                }
            }

            // Created pending indexes
            if (createdIndexes != null) {
                indexUpdateListener.createIndexes(subject, createdIndexes.toArray(new IndexDescriptor[0]));
                createdIndexes = null;
            }
        }

        @Override
        public boolean visitIndexUpdateCommand(IndexUpdateCommand command) {
            IndexDescriptor index = schemaCache.getIndex(command.getIndexId());
            if (index == null) {
                return false;
            }
            if (command instanceof TokenIndexUpdateCommand idxCommand) {
                var indexUpdate = IndexEntryUpdate.change(
                        idxCommand.getEntityId(),
                        index,
                        commandSelector.getBefore(idxCommand),
                        commandSelector.getAfter(idxCommand));
                indexUpdates.add(indexUpdate);
            } else {
                ValueIndexUpdateCommand idxCommand = (ValueIndexUpdateCommand) command;
                switch (commandSelector.mode(idxCommand)) {
                    case ADDED:
                        indexUpdates.add(IndexEntryUpdate.add(idxCommand.getEntityId(), index, idxCommand.getAfter()));
                        break;
                    case CHANGED:
                        indexUpdates.add(IndexEntryUpdate.change(
                                idxCommand.getEntityId(),
                                index,
                                commandSelector.getBefore(idxCommand),
                                commandSelector.getAfter(idxCommand)));
                        break;
                    case REMOVED:
                        indexUpdates.add(
                                IndexEntryUpdate.remove(idxCommand.getEntityId(), index, idxCommand.getAfter()));
                        break;
                    default:
                        throw new IllegalArgumentException();
                }
            }

            return false;
        }

        @Override
        public boolean visitSchemaRuleCommand(Command.SchemaRuleCommand command) {
            SchemaRule schemaRule = command.getSchemaRule();
            processSchemaCommand(command.getMode(), schemaRule);
            return false;
        }

        private void processSchemaCommand(Command.Mode commandMode, SchemaRule schemaRule) {
            if (schemaRule instanceof IndexDescriptor indexRule) {
                switch (commandMode) {
                    case UPDATE:
                        // Shouldn't we be more clear about that we are waiting for an index to come online here?
                        // right now we just assume that an update to index records means wait for it to be online.
                        if (indexRule.isUnique()) {
                            // Register activations into the IndexActivator instead of IndexingService to avoid deadlock
                            // that could ensue for applying batches of transactions where a previous transaction in the
                            // same
                            // batch acquires a low-level commit lock that prevents the very same index population to
                            // complete.
                            indexActivator.activateIndex(indexRule);
                        }
                        break;
                    case CREATE:
                        // Add to list so that all these indexes will be created in one call later
                        createdIndexes = createdIndexes == null ? new ArrayList<>() : createdIndexes;
                        createdIndexes.add(indexRule);
                        break;
                    case DELETE:
                        indexUpdateListener.dropIndex(indexRule);
                        indexActivator.indexDropped(indexRule);
                        break;
                    default:
                        throw new IllegalStateException(commandMode.name());
                }
            }
        }
    }
}
