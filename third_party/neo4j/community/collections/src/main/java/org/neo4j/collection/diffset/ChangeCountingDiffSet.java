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

public class ChangeCountingDiffSet extends RemovalsCountingDiffSets {
    private static final long CHANGE_COUNTING_DIFF_SET_SHALLOW_SIZE =
            HeapEstimator.shallowSizeOfInstance(ChangeCountingDiffSet.class);
    private final CollectionsFactory collectionsFactory;
    private final MemoryTracker memoryTracker;

    static ChangeCountingDiffSet newRemovalsCountingDiffSets(
            CollectionsFactory collectionsFactory, MemoryTracker memoryTracker) {
        memoryTracker.allocateHeap(CHANGE_COUNTING_DIFF_SET_SHALLOW_SIZE);
        return new ChangeCountingDiffSet(collectionsFactory, memoryTracker);
    }

    private MutableLongSet changed;

    private ChangeCountingDiffSet(CollectionsFactory collectionsFactory, MemoryTracker memoryTracker) {
        super(collectionsFactory, memoryTracker);
        this.collectionsFactory = collectionsFactory;
        this.memoryTracker = memoryTracker;
    }

    public LongSet getChanged() {
        return changed != null ? changed : LongSets.immutable.empty();
    }

    public void change(long element) {
        assert !isAdded(element);
        assert !isRemoved(element);
        if (changed == null) {
            changed = collectionsFactory.newLongSet(memoryTracker);
        }
        changed.add(element);
    }

    public void unchange(long element) {
        assert !isAdded(element);
        assert !isRemoved(element);
        if (changed != null) {
            changed.remove(element);
        }
    }

    @Override
    public boolean remove(long elem) {
        if (changed != null) {
            changed.remove(elem);
        }
        return super.remove(elem);
    }
}
