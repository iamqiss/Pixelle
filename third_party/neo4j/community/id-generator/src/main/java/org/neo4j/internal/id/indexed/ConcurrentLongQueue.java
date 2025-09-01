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

/**
 * Concurrent primitive {@code long} queue with a fixed max capacity.
 */
interface ConcurrentLongQueue {
    /**
     * Offers value {@code v} to this queue. The value may or may not be accepted, which will be signaled by the return value.
     *
     * @param v value to offer to the queue.
     * @return {@code true} if the value was accepted, i.e. typically if there was space available in the queue, otherwise {@code false}.
     */
    boolean offer(long v);

    /**
     * Takes a value from this queue, or if no value was available returns the {@code defaultValue} which was passed in.
     *
     * @param defaultValue value to return if there was no value available to take from the queue.
     * @return next value from this queue, or the {@code defaultValue} if there was no value available to take.
     */
    long takeOrDefault(long defaultValue);

    /**
     * Takes a value from this queue if value within the given boundary, or if no value was available returns the Long.MAX_VALUE.
     *
     * @param minBoundary minimum (inclusive) range of values that we are interested in.
     * @param maxBoundary maximum (exclusive) range of values that we are interested in.
     * @return next value from this queue, or Long.MAX_VALUE if there was no value available to take.
     */
    long takeInRange(long minBoundary, long maxBoundary);

    /**
     * @return size of this queue, i.e. how many values are queued right now.
     */
    int size();

    /**
     * @return number of IDs that can be offered to this cache before it's full.
     */
    int availableSpace();

    /**
     * Clears the queue.
     */
    void clear();
}
