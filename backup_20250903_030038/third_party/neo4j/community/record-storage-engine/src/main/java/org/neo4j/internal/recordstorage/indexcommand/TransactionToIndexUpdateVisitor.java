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

import static org.apache.commons.lang3.ArrayUtils.EMPTY_INT_ARRAY;
import static org.neo4j.common.EntityType.NODE;
import static org.neo4j.common.EntityType.RELATIONSHIP;
import static org.neo4j.internal.schema.SchemaDescriptors.forAnyEntityTokens;
import static org.neo4j.storageengine.api.IndexEntryUpdate.add;
import static org.neo4j.storageengine.api.IndexEntryUpdate.change;
import static org.neo4j.storageengine.api.IndexEntryUpdate.remove;
import static org.neo4j.token.api.TokenConstants.ANY_RELATIONSHIP_TYPE;

import java.util.Arrays;
import org.eclipse.collections.api.set.primitive.IntSet;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;
import org.neo4j.common.EntityType;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.IOUtils;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.StorageRelationshipScanCursor;
import org.neo4j.storageengine.api.ValueIndexEntryUpdate;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.storageengine.api.txstate.EntityChange;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;
import org.neo4j.storageengine.api.txstate.RelationshipModifications;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;
import org.neo4j.values.storable.ValueTuple;

public class TransactionToIndexUpdateVisitor extends TxStateVisitor.Delegator {
    private static final int[] NO_TOKENS = EMPTY_INT_ARRAY;

    private final IndexRecordState indexRecordState;
    private final ReadableTransactionState txState;
    private final StorageNodeCursor nodeCursor;
    private final StorageRelationshipScanCursor relationshipCursor;
    private final IndexDescriptor labelIndex;
    private final IndexDescriptor relationshipTypeIndex;

    public TransactionToIndexUpdateVisitor(
            TxStateVisitor next,
            IndexRecordState indexRecordState,
            StorageReader storageReader,
            ReadableTransactionState txState,
            CursorContext cursorContext,
            StoreCursors storeCursors,
            MemoryTracker memoryTracker) {
        super(next);
        this.indexRecordState = indexRecordState;

        this.txState = txState;
        this.nodeCursor = storageReader.allocateNodeCursor(cursorContext, storeCursors, memoryTracker);
        this.relationshipCursor =
                storageReader.allocateRelationshipScanCursor(cursorContext, storeCursors, memoryTracker);

        labelIndex = getTokenIndex(storageReader, NODE);
        relationshipTypeIndex = getTokenIndex(storageReader, RELATIONSHIP);
    }

    private static IndexDescriptor getTokenIndex(StorageReader storageReader, EntityType entityType) {
        return Iterators.firstOrNull(storageReader.indexGetForSchema(forAnyEntityTokens(entityType)));
    }

    @Override
    public void visitDeletedNode(long id) {
        super.visitDeletedNode(id);

        if (labelIndex == null) {
            return;
        }
        nodeCursor.single(id);

        if (nodeCursor.next()) {
            int[] labelsBefore = nodeCursor.labels();
            if (labelsBefore.length > 1) {
                Arrays.sort(labelsBefore);
            }
            indexRecordState.addTokenUpdate(change(id, labelIndex, labelsBefore, NO_TOKENS));
        }
    }

    @Override
    public void visitNodeLabelChanges(long id, LongSet added, LongSet removed) throws ConstraintValidationException {
        super.visitNodeLabelChanges(id, added, removed);

        if (labelIndex == null) {
            return;
        }

        IntSet labels;
        int[] currentLabels;

        if (txState.nodeIsAddedInThisBatch(id)) {
            labels = IntHashSet.newSetWith(toIntArray(added));
            currentLabels = NO_TOKENS;
        } else {
            nodeCursor.single(id);
            if (nodeCursor.next()) {
                currentLabels = nodeCursor.labels();
            } else {
                currentLabels = NO_TOKENS;
            }

            MutableIntSet mutableLabels = new IntHashSet();
            for (int currentLabel : currentLabels) {
                if (!removed.contains(currentLabel)) {
                    mutableLabels.add(currentLabel);
                }
            }

            mutableLabels.addAll(toIntArray(added));
            labels = mutableLabels;
        }
        indexRecordState.addTokenUpdate(change(id, labelIndex, currentLabels, labels.toSortedArray()));
    }

    @Override
    public void visitRelationshipModifications(RelationshipModifications modifications)
            throws ConstraintValidationException {
        super.visitRelationshipModifications(modifications);

        if (relationshipTypeIndex == null) {
            return;
        }

        modifications
                .creations()
                .forEach((id, type, startNode, endNode, addedProperties, changedProperties, removedProperties) ->
                        indexRecordState.addTokenUpdate(
                                change(id, relationshipTypeIndex, NO_TOKENS, new int[] {type})));
        modifications
                .deletions()
                .forEach((id, type, startNode, endNode, noProperties, changedProperties, removedProperties) -> {
                    if (type == ANY_RELATIONSHIP_TYPE) {
                        relationshipCursor.single(id);
                        if (!relationshipCursor.next()) {
                            throw new IllegalStateException(
                                    "Relationship being deleted should exist along with its nodes. Relationship[" + id
                                            + "]");
                        }
                        indexRecordState.addTokenUpdate(
                                change(id, relationshipTypeIndex, new int[] {relationshipCursor.type()}, NO_TOKENS));
                    } else {
                        indexRecordState.addTokenUpdate(change(id, relationshipTypeIndex, new int[] {type}, NO_TOKENS));
                    }
                });
    }

    @Override
    public void visitValueIndexUpdate(
            IndexDescriptor descriptor, long entityId, ValueTuple values, EntityChange entityChange) {
        super.visitValueIndexUpdate(descriptor, entityId, values, entityChange);
        var key = new IndexRecordState.IndexEntityPair(descriptor.getId(), entityId);
        ValueIndexEntryUpdate<IndexDescriptor> existingUpdate = indexRecordState.getValueUpdate(key);

        var update = getValueUpdate(descriptor, entityId, values, entityChange, existingUpdate, key);
        indexRecordState.putValueUpdate(key, update);
    }

    private ValueIndexEntryUpdate<IndexDescriptor> getValueUpdate(
            IndexDescriptor descriptor,
            long entityId,
            ValueTuple values,
            EntityChange entityChange,
            ValueIndexEntryUpdate<IndexDescriptor> existingUpdate,
            IndexRecordState.IndexEntityPair key) {
        if (entityChange == EntityChange.ADDED) {
            return existingUpdate == null
                    ? add(entityId, descriptor, values.getValues())
                    : change(entityId, descriptor, existingUpdate.values(), values.getValues());
        }
        return existingUpdate == null
                ? remove(entityId, descriptor, values.getValues())
                : change(entityId, descriptor, values.getValues(), existingUpdate.values());
    }

    @Override
    public void close() throws KernelException {
        super.close();
        IOUtils.closeAllUnchecked(nodeCursor, relationshipCursor);
    }

    private static int[] toIntArray(LongSet ids) {
        return toIntArray(ids.toArray());
    }

    private static int[] toIntArray(long[] data) {
        final var tokens = new int[data.length];
        for (var i = 0; i < tokens.length; i++) {
            tokens[i] = (int) data[i];
        }
        return tokens;
    }
}
