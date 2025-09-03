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
package org.neo4j.collection.diffset;

import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.neo4j.collection.factory.CollectionsFactory;
import org.neo4j.memory.HeapEstimator;
import org.neo4j.memory.MemoryTracker;

/**
 * This class works around the fact that create-delete in the same transaction is a no-op in {@link MutableDiffSetsImpl},
 * whereas we need to know total number of explicit removals.
 */
public class RemovalsCountingDiffSets extends MutableLongDiffSetsImpl {
    private static final long REMOVALS_COUNTING_DIFF_SET_SHALLOW_SIZE =
            HeapEstimator.shallowSizeOfInstance(RemovalsCountingDiffSets.class);

    private final CollectionsFactory collectionsFactory;
    private final MemoryTracker memoryTracker;
    private MutableLongSet removedFromAdded;

    static RemovalsCountingDiffSets newRemovalsCountingDiffSets(
            CollectionsFactory collectionsFactory, MemoryTracker memoryTracker) {
        memoryTracker.allocateHeap(REMOVALS_COUNTING_DIFF_SET_SHALLOW_SIZE);
        return new RemovalsCountingDiffSets(collectionsFactory, memoryTracker);
    }

    protected RemovalsCountingDiffSets(CollectionsFactory collectionsFactory, MemoryTracker memoryTracker) {
        super(collectionsFactory, memoryTracker);
        this.collectionsFactory = collectionsFactory;
        this.memoryTracker = memoryTracker;
    }

    @Override
    public boolean remove(long elem) {
        if (isAdded(elem) && super.remove(elem)) {
            if (removedFromAdded == null) {
                removedFromAdded = collectionsFactory.newLongSet(memoryTracker);
            }
            removedFromAdded.add(elem);
            return true;
        }
        return super.remove(elem);
    }

    @Override
    public LongSet getRemovedFromAdded() {
        return removedFromAdded != null ? removedFromAdded : LongSets.immutable.empty();
    }

    public boolean wasRemoved(long id) {
        return (removedFromAdded != null && removedFromAdded.contains(id)) || super.isRemoved(id);
    }
}
