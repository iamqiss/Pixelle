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
package org.neo4j.procedure.impl.memory;

import org.neo4j.collection.trackable.HeapTracking;
import org.neo4j.collection.trackable.HeapTrackingArrayList;
import org.neo4j.collection.trackable.HeapTrackingUnifiedMap;
import org.neo4j.function.ThrowingFunction;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.procedure.memory.HeapEstimator;
import org.neo4j.procedure.memory.ProcedureMemory;
import org.neo4j.procedure.memory.ProcedureMemoryTracker;

public class ProcedureMemoryProvider implements ThrowingFunction<Context, ProcedureMemory, ProcedureException> {
    @Override
    public ProcedureMemory apply(Context context) throws ProcedureException {
        return new ProcedureMemoryImpl(context.procedureCallContext().memoryTracker());
    }
}

class ProcedureMemoryImpl implements ProcedureMemory {
    private final MemoryTracker innerTracker;

    ProcedureMemoryImpl(MemoryTracker txMemoryTracker) {
        this.innerTracker = txMemoryTracker;
    }

    @Override
    public ProcedureMemoryTracker newTracker() {
        return new ProcedureMemoryTrackerImpl(innerTracker.getScopedMemoryTracker());
    }

    @Override
    public HeapEstimator heapEstimator() {
        return ProcedureHeapEstimator.INSTANCE;
    }

    @Override
    public HeapTrackingCollectionFactory collections() {
        return new HeapTrackingCollectionFactory() {
            @Override
            public <V> HeapTracking.List<V> newHeapTrackingArrayList() {
                return HeapTrackingArrayList.newArrayList(innerTracker);
            }

            @Override
            public <V> HeapTracking.List<V> newHeapTrackingArrayList(int initialCapacity) {
                return HeapTrackingArrayList.newArrayList(initialCapacity, innerTracker);
            }

            @Override
            public <K, V> HeapTracking.Map<K, V> newHeapTrackingUnifiedMap() {
                return HeapTrackingUnifiedMap.createUnifiedMap(innerTracker);
            }

            @Override
            public <K, V> HeapTracking.Map<K, V> newHeapTrackingUnifiedMap(int initialCapacity) {
                return HeapTrackingUnifiedMap.createUnifiedMap(initialCapacity, innerTracker);
            }
        };
    }
}

class ProcedureMemoryTrackerImpl implements ProcedureMemoryTracker {
    private final MemoryTracker internalMemoryTracker;

    ProcedureMemoryTrackerImpl(MemoryTracker internalMemoryTracker) {
        this.internalMemoryTracker = internalMemoryTracker;
    }

    @Override
    public void allocateHeap(long bytes) {
        internalMemoryTracker.allocateHeap(bytes);
    }

    @Override
    public void releaseHeap(long bytes) {
        internalMemoryTracker.releaseHeap(bytes);
    }

    @Override
    public void close() {
        internalMemoryTracker.close();
    }
}
