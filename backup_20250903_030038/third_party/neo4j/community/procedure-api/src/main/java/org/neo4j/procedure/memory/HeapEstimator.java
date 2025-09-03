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

// Candidate for @PublicApi
public interface HeapEstimator {
    /**
     * Returns an estimate of the shallow heap memory usage in bytes of the specified object.
     * Note, this is usually an expensive operation.
     */
    long shallowSize(Object o);

    /**
     * Returns an estimate of the shallow instance size in bytes that an instance of the given class would occupy.
     * This works with all conventional classes and primitive types,
     * but not with arrays (the size then depends on the number of elements and varies from object to object).
     *
     * @throws IllegalArgumentException if {@code clazz} is an array class.
     * @see #shallowSize(Object)
     */
    long shallowSizeOfInstance(Class<?> cls);

    /** Returns an estimate of the shallow heap memory usage in bytes of an object array with the specified size. */
    long shallowSizeOfObjectArray(int size);

    /** Returns the heap memory usage in bytes of a primitive byte array with the specified size. */
    long sizeOfByteArray(int size);

    /** Returns the heap memory usage in bytes of a primitive int array with the specified size. */
    long sizeOfIntArray(int size);

    /** Returns the heap memory usage in bytes of a primitive long array with the specified size. */
    long sizeOfLongArray(int size);

    /** Returns the heap memory usage in bytes of a primitive float array with the specified size. */
    long sizeOfFloatArray(int size);

    /** Returns the heap memory usage in bytes of a primitive double array with the specified size. */
    long sizeOfDoubleArray(int size);
}
