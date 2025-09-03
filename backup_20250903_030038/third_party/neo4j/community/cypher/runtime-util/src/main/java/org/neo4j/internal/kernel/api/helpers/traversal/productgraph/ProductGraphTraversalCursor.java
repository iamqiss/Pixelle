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

import java.util.List;
import org.apache.commons.lang3.ArrayUtils;
import org.neo4j.exceptions.EntityNotFoundException;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.internal.kernel.api.KernelReadTracer;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalEntities;
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.TraversalDirection;
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.PPBFSHooks;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.DirectedTypes;
import org.neo4j.storageengine.api.RelationshipDirection;
import org.neo4j.storageengine.api.RelationshipSelection;

public class ProductGraphTraversalCursor implements AutoCloseable {

    private final DataGraphRelationshipCursor graphCursor;
    private final RelationshipExpansionCursor relationshipExpansionCursor;
    private boolean initialized = false;
    private final DirectedTypes directedTypes;
    private final ComposedSourceCursor<List<State>, State, RelationshipExpansion> nfaCursor;
    private TraversalDirection direction = TraversalDirection.FORWARD;

    public ProductGraphTraversalCursor(
            Read read,
            NodeCursor nodeCursor,
            RelationshipTraversalCursor relCursor,
            MemoryTracker mt,
            PPBFSHooks hooks) {
        this(new DataGraphRelationshipCursorImpl(read, nodeCursor, relCursor, hooks), mt);
    }

    public ProductGraphTraversalCursor(DataGraphRelationshipCursor graph, MemoryTracker mt) {
        this.graphCursor = graph;
        this.relationshipExpansionCursor = new RelationshipExpansionCursor();
        this.nfaCursor = new ComposedSourceCursor<>(new ListCursor<>(), relationshipExpansionCursor);
        this.directedTypes = new DirectedTypes(mt);
    }

    public State targetState() {
        return nfaCursor.current().targetState();
    }

    public State currentInputState() {
        return nfaCursor.currentIntermediate();
    }

    public long otherNodeReference() {
        return graphCursor.otherNodeReference();
    }

    public long relationshipReference() {
        return graphCursor.relationshipReference();
    }

    public RelationshipExpansion relationshipExpansion() {
        return nfaCursor.current();
    }

    public boolean next() {
        if (!initialized) {
            if (!nextRelationship()) {
                return false;
            }
            initialized = true;
        }

        while (true) {
            while (nfaCursor.next()) {
                if (evaluateCurrent()) {
                    return true;
                }
            }

            if (!nextRelationship()) {
                return false;
            }
        }
    }

    private boolean nextRelationship() {
        nfaCursor.reset();
        return graphCursor.nextRelationship();
    }

    private boolean evaluateCurrent() {
        var expansion = nfaCursor.current();
        var expansionDir =
                switch (direction) {
                    case FORWARD -> expansion.direction();
                    case BACKWARD -> expansion.direction().reverse();
                };
        return graphCursor.direction().matches(expansionDir)
                && (expansion.types() == null || ArrayUtils.contains(expansion.types(), graphCursor.type()))
                && expansion.testRelationship(graphCursor, direction)
                && expansion.endState(direction).test(graphCursor.otherNodeReference());
    }

    public void setNodeAndStates(long nodeId, List<State> states, TraversalDirection direction) {
        this.direction = direction;
        initialized = false;
        this.nfaCursor.setSource(states);
        this.relationshipExpansionCursor.setDirection(direction);

        // preprocess nfa type directions for the current node for use in the graph cursor
        directedTypes.clear();
        while (this.nfaCursor.next()) {
            var expansion = this.nfaCursor.current();
            var expansionDir =
                    switch (direction) {
                        case FORWARD -> expansion.direction();
                        case BACKWARD -> expansion.direction().reverse();
                    };
            directedTypes.addTypes(expansion.types(), expansionDir);
        }
        this.nfaCursor.reset();

        this.graphCursor.setNode(nodeId, RelationshipSelection.selection(directedTypes));
    }

    public void setTracer(KernelReadTracer tracer) {
        graphCursor.setTracer(tracer);
    }

    @Override
    public void close() throws Exception {
        // this class does not own the nodeCursor or traversalCursor, they should be closed by the consumer
        this.nfaCursor.close();
    }

    public interface DataGraphRelationshipCursor extends RelationshipTraversalEntities {
        boolean nextRelationship();

        void setNode(long nodeId, RelationshipSelection relationshipSelection);

        default RelationshipDirection direction() {
            return RelationshipDirection.directionOfStrict(
                    originNodeReference(), sourceNodeReference(), targetNodeReference());
        }

        void setTracer(KernelReadTracer tracer);
    }

    public static class DataGraphRelationshipCursorImpl implements DataGraphRelationshipCursor {
        private final Read read;
        private final NodeCursor node;
        private final RelationshipTraversalCursor rel;
        private final PPBFSHooks hooks;

        public DataGraphRelationshipCursorImpl(
                Read read, NodeCursor node, RelationshipTraversalCursor rel, PPBFSHooks hooks) {
            this.read = read;
            this.node = node;
            this.rel = rel;
            this.hooks = hooks;
        }

        @Override
        public boolean nextRelationship() {
            hooks.cursorNextRelationship(node.nodeReference());
            return rel.next();
        }

        @Override
        public void setTracer(KernelReadTracer tracer) {
            node.setTracer(tracer);
            rel.setTracer(tracer);
        }

        @Override
        public void setNode(long nodeId, RelationshipSelection relationshipSelection) {
            hooks.cursorSetNode(nodeId);
            read.singleNode(nodeId, node);
            if (!node.next()) {
                var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_25N11)
                        .build();
                throw new EntityNotFoundException(gql, "Node " + nodeId + " was unexpectedly deleted");
            }
            node.relationships(rel, relationshipSelection);
        }

        @Override
        public long relationshipReference() {
            return rel.reference();
        }

        @Override
        public long originNodeReference() {
            return rel.originNodeReference();
        }

        @Override
        public long otherNodeReference() {
            return rel.otherNodeReference();
        }

        @Override
        public long sourceNodeReference() {
            return rel.sourceNodeReference();
        }

        @Override
        public long targetNodeReference() {
            return rel.targetNodeReference();
        }

        @Override
        public int type() {
            return rel.type();
        }
    }
}
