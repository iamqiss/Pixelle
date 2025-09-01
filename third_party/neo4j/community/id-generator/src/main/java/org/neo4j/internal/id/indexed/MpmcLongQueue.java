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
package org.neo4j.internal.id.indexed;

import static java.lang.Math.max;
import static java.lang.Math.toIntExact;
import static org.neo4j.util.Preconditions.requirePowerOfTwo;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Multi Producer Multiple Consumers FIFO queue.
 */
class MpmcLongQueue implements ConcurrentLongQueue {
    private static final long EMPTY_VALUE = -1;

    private final long idxMask;
    private final AtomicLongArray array;
    private final AtomicLong readSeq = new AtomicLong();
    private final AtomicLong writeSeq = new AtomicLong();

    MpmcLongQueue(int capacity) {
        requirePowerOfTwo(capacity);
        this.idxMask = capacity - 1;
        var array = new long[capacity];
        Arrays.fill(array, EMPTY_VALUE);
        this.array = new AtomicLongArray(array);
    }

    @Override
    public boolean offer(long value) {
        assert value != EMPTY_VALUE;
        long currentWriteSeq;
        long currentReadSeq;
        int writeIdx;
        do {
            currentWriteSeq = writeSeq.get();
            currentReadSeq = readSeq.get();
            writeIdx = idx(currentWriteSeq);
            var readIdx = idx(currentReadSeq);
            if (writeIdx == readIdx && currentWriteSeq != currentReadSeq) {
                return false;
            }
        } while (array.get(writeIdx) != EMPTY_VALUE || !writeSeq.compareAndSet(currentWriteSeq, currentWriteSeq + 1));
        array.set(writeIdx, value);
        return true;
    }

    @Override
    public long takeOrDefault(long defaultValue) {
        long currentReadSeq;
        long currentWriteSeq;
        long value;
        int idx;
        do {
            currentReadSeq = readSeq.get();
            currentWriteSeq = writeSeq.get();
            if (currentReadSeq == currentWriteSeq) {
                return defaultValue;
            }
            idx = idx(currentReadSeq);
            value = array.get(idx);
        } while (value == EMPTY_VALUE || !readSeq.compareAndSet(currentReadSeq, currentReadSeq + 1));
        array.set(idx, EMPTY_VALUE);
        return value;
    }

    @Override
    public long takeInRange(long minBoundary, long maxBoundary) {
        long currentReadSeq;
        long currentWriteSeq;
        long value;
        int idx;
        do {
            currentReadSeq = readSeq.get();
            currentWriteSeq = writeSeq.get();
            if (currentReadSeq == currentWriteSeq) {
                return Long.MAX_VALUE;
            }
            idx = idx(currentReadSeq);
            value = array.get(idx);
            if (value >= maxBoundary || value < minBoundary) {
                return Long.MAX_VALUE;
            }
        } while (value == EMPTY_VALUE || !readSeq.compareAndSet(currentReadSeq, currentReadSeq + 1));
        array.set(idx, EMPTY_VALUE);
        return value;
    }

    @Override
    public int size() {
        // Why do we need max on this value? Well the size being returned is a rough estimate since we're reading two
        // atomic longs un-atomically.
        // We may end up in a scenario where writeSeq is read and then both writeSeq as well as readSeq moves along so
        // that when later
        // reading readSeq it will be bigger than writeSeq. This is fine, but would look strange on the receiving end,
        // so let it be 0 instead.
        return toIntExact(max(0, writeSeq.get() - readSeq.get()));
    }

    @Override
    public int availableSpace() {
        return array.length() - size();
    }

    /**
     * This call is not thread-safe w/ concurrent calls to {@link #offer(long)} so external synchronization is required.
     */
    @Override
    public void clear() {
        readSeq.set(writeSeq.get());
    }

    private int idx(long seq) {
        return toIntExact(seq & idxMask);
    }
}
