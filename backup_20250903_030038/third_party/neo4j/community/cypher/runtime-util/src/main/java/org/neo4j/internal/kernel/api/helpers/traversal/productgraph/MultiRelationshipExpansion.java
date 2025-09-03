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
package org.neo4j.internal.kernel.api.helpers.traversal.productgraph;

import java.util.function.LongPredicate;
import java.util.function.Predicate;
import org.neo4j.graphdb.Direction;
import org.neo4j.internal.kernel.api.RelationshipTraversalEntities;
import org.neo4j.internal.kernel.api.helpers.traversal.SlotOrName;
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.TraversalDirection;
import org.neo4j.storageengine.api.RelationshipSelection;
import org.neo4j.values.virtual.VirtualRelationshipValue;

public record MultiRelationshipExpansion(
        State sourceState, Rel[] rels, Node[] nodes, CompoundPredicate compoundPredicate, State targetState)
        implements Transition {

    public MultiRelationshipExpansion {
        assert nodes.length == rels.length - 1
                : "There must be exactly one fewer interior nodes than relationships in a MultiRelationshipExpansion";
    }

    public int length() {
        return rels.length;
    }

    public Rel rel(int depth, TraversalDirection direction) {
        return direction.isBackward() ? rels[this.rels.length - 1 - depth] : rels[depth];
    }

    public Node node(int depth, TraversalDirection direction) {
        return direction.isBackward() ? nodes[this.nodes.length - 1 - depth] : nodes[depth];
    }

    public LongPredicate nodePredicate(int depth, TraversalDirection direction) {
        if (depth == nodes.length) {
            return direction.isBackward() ? sourceState.predicate() : targetState.predicate();
        }
        return node(depth, direction).predicate();
    }

    public record Node(LongPredicate predicate, SlotOrName slotOrName) {}

    public record Rel(
            Predicate<RelationshipTraversalEntities> predicate,
            int[] types,
            Direction direction,
            SlotOrName slotOrName) {
        public RelationshipSelection getSelection(TraversalDirection traversalDirection) {
            return RelationshipSelection.selection(types, getDirection(traversalDirection));
        }

        public Direction getDirection(TraversalDirection traversalDirection) {
            return traversalDirection == TraversalDirection.BACKWARD ? direction.reverse() : direction;
        }
    }

    public interface CompoundPredicate {
        boolean test(long startNode, VirtualRelationshipValue[] rels, long[] interiorNodes, long endNode);

        CompoundPredicate ALWAYS_TRUE = (startNode, rels, interiorNodes, endNode) -> true;
    }
}
