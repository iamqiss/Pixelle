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
package org.neo4j.procedure.memory;

import org.neo4j.collection.trackable.HeapTracking;

// Candidate for @PublicApi
public interface ProcedureMemory {
    /**
     * Returns a new memory tracker with the same lifetime as the current transaction.
     * All allocations registered in the returned tracker will be automatically freed when the tracker or transaction is closed.
     * The tracker may only be accessed by the thread that calls the procedure or function!
     * Registered allocations will count towards the transaction limits set by db.memory.transaction.*.
     */
    ProcedureMemoryTracker newTracker();

    // TODO ProcedureMemoryTracker newThreadSafeTracker();

    HeapEstimator heapEstimator();

    HeapTrackingCollectionFactory collections();

    // Candidate for @PublicApi
    interface HeapTrackingCollectionFactory {

        /**
         * Return a new array list that tracks the estimated heap size of its internal structure in a new memory tracker.
         * Note, the size of elements in the list are NOT tracked.
         * Note, do not support {@link java.util.List#subList(int, int)}.
         *
         * @param initialCapacity initial capacity
         */
        <V> HeapTracking.List<V> newHeapTrackingArrayList(int initialCapacity);

        /**
         * Return a new array list that tracks the estimated heap size of its internal structure in a new memory tracker.
         * Note, the size of elements in the list are NOT tracked.
         * Note, do not support {@link java.util.List#subList(int, int)}.
         */
        <V> HeapTracking.List<V> newHeapTrackingArrayList();

        /**
         * Return a map, that stores key/value pairs in a single array, where alternate slots are keys and values.
         * The map tracks the estimated heap size of its internal structure in a new memory tracker.
         * Note, the size of keys and values in the map are NOT tracked.
         *
         * @param initialCapacity initial capacity
         */
        <K, V> HeapTracking.Map<K, V> newHeapTrackingUnifiedMap(int initialCapacity);

        /**
         * Return a map, that stores key/value pairs in a single array, where alternate slots are keys and values.
         * The map tracks the estimated heap size of its internal structure in a new memory tracker.
         * Note, the size of keys and values in the map are NOT tracked.
         */
        <K, V> HeapTracking.Map<K, V> newHeapTrackingUnifiedMap();
    }
}
