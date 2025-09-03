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
package org.neo4j.storageengine.api;

import org.eclipse.collections.api.IntIterable;

/**
 * Visitor of relationship data.
 *
 * @param <EXCEPTION> exception thrown from the {@link #visit(long, int, long, long, Iterable, Iterable, IntIterable)} method.
 */
public interface RelationshipVisitorWithProperties<EXCEPTION extends Exception> {
    /**
     * Visits data about a relationship.
     *
     * @param relationshipId    relationship id to visit data for.
     * @param typeId            relationship type id for the relationship.
     * @param startNodeId       id of start node of the relationship.
     * @param endNodeId         id of the end node of the relationship.
     * @param addedProperties   added properties for this relationship.
     * @param changedProperties changed properties for this relationship.
     * @param removedProperties removed properties for this relationship.
     */
    void visit(
            long relationshipId,
            int typeId,
            long startNodeId,
            long endNodeId,
            Iterable<StorageProperty> addedProperties,
            Iterable<StorageProperty> changedProperties,
            IntIterable removedProperties)
            throws EXCEPTION;
}
