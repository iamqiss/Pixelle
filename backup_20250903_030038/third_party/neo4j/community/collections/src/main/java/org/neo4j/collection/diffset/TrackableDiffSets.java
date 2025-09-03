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

import static org.neo4j.collection.diffset.MutableLongDiffSetsImpl.createMutableLongDiffSetsImpl;

import org.neo4j.collection.factory.CollectionsFactory;
import org.neo4j.memory.MemoryTracker;

public class TrackableDiffSets {
    public static MutableLongDiffSets newMutableLongDiffSets(
            CollectionsFactory collectionsFactory, MemoryTracker memoryTracker) {
        return createMutableLongDiffSetsImpl(collectionsFactory, memoryTracker);
    }

    public static RemovalsCountingDiffSets newRemovalsCountingDiffSets(
            CollectionsFactory collectionsFactory, MemoryTracker memoryTracker) {
        return RemovalsCountingDiffSets.newRemovalsCountingDiffSets(collectionsFactory, memoryTracker);
    }

    public static ChangeCountingDiffSet newChangeCountingDiffSets(
            CollectionsFactory collectionsFactory, MemoryTracker memoryTracker) {
        return ChangeCountingDiffSet.newRemovalsCountingDiffSets(collectionsFactory, memoryTracker);
    }

    public static <T> MutableDiffSets<T> newMutableDiffSets(MemoryTracker memoryTracker) {
        return MutableDiffSetsImpl.newMutableDiffSets(memoryTracker);
    }
}
