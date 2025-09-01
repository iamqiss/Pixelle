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
package org.neo4j.collection.trackable;

// Candidate for @PublicApi
public interface HeapTracking {
    /**
     * A list that tracks the estimated heap size of its internal structure on an internal memory tracker.
     * Do NOT track the heap size of the containing items.
     */
    interface List<T> extends java.util.List<T>, AutoCloseable {
        /** Release all registered heap memory of this list. */
        @Override
        void close();
    }

    /**
     * A map that tracks the estimated heap size of its internal structure on an internal memory tracker.
     * Do NOT track the heap size of the containing key and values.
     */
    interface Map<K, V> extends java.util.Map<K, V>, AutoCloseable {
        /** Release all registered heap memory of this map. */
        @Override
        void close();
    }
}
