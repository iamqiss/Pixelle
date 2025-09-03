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
package org.neo4j.dbms.systemgraph.allocation;

/**
 * Simple record type representing how "resource intensive" a user expects a database to be.
 * </p>
 * This can then be used by the "equal weights" database allocator (EqualNumbersDatabaseAllocatorProvider) to attempt to achieve balanced resource
 * usage across a cluster.
 * </p>
 * If using the "equal weights" allocator, the larger a database's weight value, the less likely it is to be allocated to servers alongside other databases
 * with large weight values.
 *
 * @param weight a whole number greater than or equal to zero
 */
public record DatabaseWeight(int weight) implements DatabaseAllocationHints.Hint<Integer>, Comparable<DatabaseWeight> {
    public static final DatabaseWeight DEFAULT_WEIGHT = new DatabaseWeight(10);
    public static final String KEY = "weight";

    public DatabaseWeight {
        if (weight < 0) {
            throw new IllegalArgumentException("A database's weight must be a positive integer");
        }
    }

    @Override
    public Integer getValue() {
        return weight;
    }

    @Override
    public String getKey() {
        return KEY;
    }

    @Override
    public int compareTo(DatabaseWeight other) {
        return Integer.compare(weight, other.weight);
    }
}
