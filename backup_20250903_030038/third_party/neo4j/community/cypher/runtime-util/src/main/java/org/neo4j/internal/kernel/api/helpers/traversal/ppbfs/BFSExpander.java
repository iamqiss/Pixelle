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
package org.neo4j.internal.kernel.api.helpers.traversal.ppbfs;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.LongPredicate;
import java.util.function.Predicate;
import org.neo4j.collection.trackable.HeapTracking;
import org.neo4j.collection.trackable.HeapTrackingArrayList;
import org.neo4j.collection.trackable.HeapTrackingCollections;
import org.neo4j.collection.trackable.HeapTrackingLongArrayList;
import org.neo4j.collection.trackable.HeapTrackingUnifiedMap;
import org.neo4j.function.Predicates;
import org.neo4j.graphdb.Direction;
import org.neo4j.internal.helpers.collection.PrefetchingIterator;
import org.neo4j.internal.kernel.api.KernelReadTracer;
import org.neo4j.internal.kernel.api.RelationshipTraversalEntities;
import org.neo4j.internal.kernel.api.helpers.traversal.ReversedRelTraversalEntities;
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.PPBFSHooks;
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.MultiRelationshipExpansion;
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.ProductGraphTraversalCursor;
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.RelationshipPredicate;
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.State;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.memory.HeapEstimator;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.RelationshipSelection;
import org.neo4j.values.virtual.VirtualRelationshipValue;

final class BFSExpander implements AutoCloseable {
    private final MemoryTracker mt;
    private final PPBFSHooks hooks;
    private final GlobalState globalState;
    private final ProductGraphTraversalCursor pgCursor;
    private final CachingRelCursor relCursor;
    private final long intoTarget;
    private final TraversalMatchModeFactory tracker;

    // allocated once and reused per source nodeState
    private final HeapTrackingArrayList<State> statesList;
    private final FoundNodes foundNodes;

    record CachedRelPredicate(Predicate<RelationshipTraversalEntities> predicate, long rel, TraversalDirection dir) {
        public static long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(CachedRelPredicate.class);
    }

    record CachedNodePredicate(LongPredicate predicate, long node) {
        public static long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(CachedNodePredicate.class);
    }

    private final HeapTrackingUnifiedMap<CachedRelPredicate, Boolean> relPredicateCache;
    private final HeapTrackingUnifiedMap<CachedNodePredicate, Boolean> nodePredicateCache;

    public BFSExpander(
            FoundNodes foundNodes,
            GlobalState globalState,
            ProductGraphTraversalCursor pgCursor,
            ProductGraphTraversalCursor.DataGraphRelationshipCursor relCursor,
            long intoTarget,
            int nfaStateCount,
            TraversalMatchModeFactory tracker) {
        this.mt = globalState.mt;
        this.hooks = globalState.hooks;
        this.globalState = globalState;
        this.pgCursor = pgCursor;
        this.relCursor = new CachingRelCursor(relCursor, mt);
        this.intoTarget = intoTarget;
        this.statesList = HeapTrackingArrayList.newArrayList(nfaStateCount, mt);
        this.foundNodes = foundNodes;
        this.tracker = tracker;
        this.relPredicateCache = HeapTrackingCollections.newMap(mt);
        this.nodePredicateCache = HeapTrackingCollections.newMap(mt);
    }

    /** discover a nodeState that has not been seen before */
    public void discover(NodeState node, TraversalDirection direction) {
        hooks.discover(node, direction);
        foundNodes.addToBuffer(node);
        node.discover(direction);

        var state = node.state();

        for (var nj : state.getNodeJuxtapositions(direction)) {
            if (nj.endState(direction).test(node.id())) {
                switch (direction) {
                    case FORWARD -> {
                        var nextNode = encounter(node.id(), nj.targetState(), direction);
                        var signpost = TwoWaySignpost.fromNodeJuxtaposition(
                                mt, node, nextNode, foundNodes.forwardDepth(), tracker.lengths());
                        if (globalState.searchMode == SearchMode.Unidirectional
                                || !nextNode.hasSourceSignpost(signpost)) {
                            nextNode.addSourceSignpost(signpost, foundNodes.forwardDepth());
                        }
                    }

                    case BACKWARD -> {
                        var nextNode = encounter(node.id(), nj.sourceState(), direction);
                        var signpost = TwoWaySignpost.fromNodeJuxtaposition(mt, nextNode, node, tracker.lengths());

                        if (!nextNode.hasTargetSignpost(signpost)) {
                            var addedSignpost = node.upsertSourceSignpost(signpost);
                            addedSignpost.setMinTargetDistance(
                                    foundNodes.backwardDepth(), PGPathPropagatingBFS.Phase.Expansion);
                        }
                    }
                }
            }
        }
    }

    /** encounter a nodeState that may or may not have been seen before */
    public NodeState encounter(long nodeId, State state, TraversalDirection direction) {
        var nodeState = foundNodes.get(nodeId, state.id());

        if (nodeState == null) {
            nodeState = new NodeState(globalState, nodeId, state, intoTarget, tracker.lengths());
            discover(nodeState, direction);
        } else if (globalState.searchMode == SearchMode.Bidirectional && !nodeState.hasBeenSeen(direction)) {
            // this branch means we continue expanding in both directions past the opposite frontier, if the node has
            // not been previously seen by *this* direction
            discover(nodeState, direction);
        }

        return nodeState;
    }

    private boolean cachedRelPredicate(
            Predicate<RelationshipTraversalEntities> predicate,
            RelationshipTraversalEntities rel,
            TraversalDirection dir) {
        if (predicate == RelationshipPredicate.ALWAYS_TRUE) {
            return true;
        }
        return relPredicateCache.getIfAbsentPut(
                new CachedRelPredicate(predicate, rel.relationshipReference(), dir), () -> {
                    this.mt.allocateHeap(CachedRelPredicate.SHALLOW_SIZE);
                    return predicate.test(dir.isForward() ? rel : new ReversedRelTraversalEntities(rel));
                });
    }

    private boolean cachedNodePredicate(LongPredicate predicate, long node) {
        if (predicate == Predicates.ALWAYS_TRUE_LONG) {
            return true;
        }
        return nodePredicateCache.getIfAbsentPut(new CachedNodePredicate(predicate, node), () -> {
            this.mt.allocateHeap(CachedNodePredicate.SHALLOW_SIZE);
            return predicate.test(node);
        });
    }

    private void multiHopDFS(NodeState startNode, MultiRelationshipExpansion expansion, TraversalDirection direction) {
        var rels = new VirtualRelationshipValue[expansion.length()];
        var nodes = new long[expansion.length() - 1];

        var nodeTree = new HeapTrackingLongArrayList[expansion.length() + 1];
        nodeTree[0] = HeapTrackingLongArrayList.newLongArrayList(1, mt);
        nodeTree[0].add(startNode.id());
        HeapTrackingArrayList<VirtualRelationshipValue>[] relTree = new HeapTrackingArrayList[expansion.length()];

        int depth = 0;
        MREValidator mreValidator = tracker.mreValidator();
        while (depth != -1) {
            assert depth <= expansion.length()
                    : "Multi-hop depth first search should never exceed total expansion length";
            if (nodeTree[depth] == null || nodeTree[depth].isEmpty()) {
                // exhausted this branch, backtrack up by 1 level
                if (depth > 0) {
                    rels[direction.isBackward() ? (rels.length - depth) : depth - 1] = null;

                    if (depth <= nodes.length) {
                        nodes[direction.isBackward() ? (nodes.length - depth) : depth - 1] = 0;
                    }
                }

                depth--;
            } else if (depth == expansion.length()) {
                // reached the final node of the traversal; evaluate the path and add a signpost if necessary
                var endNode = nodeTree[depth].removeLast();
                var rel = relTree[depth - 1].removeLast();
                rels[direction.isBackward() ? (rels.length - depth) : depth - 1] = rel;

                var start = direction.isBackward() ? endNode : startNode.id();
                var end = direction.isBackward() ? startNode.id() : endNode;

                if (expansion.compoundPredicate().test(start, rels, nodes, end)) {
                    var nextNode = encounter(endNode, expansion.endState(direction), direction);

                    var relIds = new long[rels.length];
                    for (int i = 0; i < rels.length; i++) {
                        relIds[i] = rels[i].id();
                    }

                    switch (direction) {
                        case FORWARD -> {
                            var signpost = TwoWaySignpost.fromMultiRel(
                                    mt,
                                    startNode,
                                    relIds,
                                    nodes.clone(),
                                    expansion,
                                    nextNode,
                                    foundNodes.forwardDepth(),
                                    tracker.lengths());
                            if (globalState.searchMode == SearchMode.Unidirectional
                                    || !nextNode.hasSourceSignpost(signpost)) {
                                nextNode.addSourceSignpost(signpost, foundNodes.forwardDepth());
                            }
                        }
                        case BACKWARD -> {
                            var signpost = TwoWaySignpost.fromMultiRel(
                                    mt, nextNode, relIds, nodes.clone(), expansion, startNode, tracker.lengths());
                            if (!nextNode.hasTargetSignpost(signpost)) {
                                var addedSignpost = startNode.upsertSourceSignpost(signpost);
                                addedSignpost.setMinTargetDistance(
                                        foundNodes.backwardDepth(), PGPathPropagatingBFS.Phase.Expansion);
                            }
                        }
                    }
                }

            } else {
                // traverse deeper into the tree
                var node = nodeTree[depth].removeLast();
                if (depth > 0) {
                    var rel = relTree[depth - 1].removeLast();
                    rels[direction.isBackward() ? (rels.length - depth) : depth - 1] = rel;

                    if (depth <= nodes.length) {
                        nodes[direction.isBackward() ? (nodes.length - depth) : depth - 1] = node;
                    }
                }

                var relHop = expansion.rel(depth, direction);
                var nodePredicate = expansion.nodePredicate(depth, direction);

                boolean canExpand = false;
                for (var it = relCursor.iterator(node, relHop.types(), relHop.getDirection(direction));
                        it.hasNext(); ) {
                    var rel = it.next();
                    if (mreValidator.validateRelationships(direction, depth, rels, rel)) {
                        if (cachedRelPredicate(relHop.predicate(), rel, direction)
                                && cachedNodePredicate(nodePredicate, rel.otherNodeReference())) {
                            if (nodeTree[depth + 1] == null) {
                                nodeTree[depth + 1] = HeapTrackingLongArrayList.newLongArrayList(mt);
                            }
                            nodeTree[depth + 1].add(rel.otherNodeReference());

                            if (relTree[depth] == null) {
                                relTree[depth] = HeapTrackingArrayList.newArrayList(mt);
                            }
                            relTree[depth].add(ValueUtils.fromRelationshipCursor(rel));
                            canExpand = true;
                        }
                    }
                }

                if (canExpand) {
                    depth++;
                }
            }
        }
    }

    public void expand() {
        foundNodes.openBuffer();
        var direction = foundNodes.getNextExpansionDirection();
        hooks.expand(direction, foundNodes);

        for (var pair : foundNodes.frontier(direction).keyValuesView()) {
            var dbNodeId = pair.getOne();
            var statesById = pair.getTwo();

            statesList.clear();
            for (var nodeState : statesById) {
                if (nodeState != null) {
                    statesList.add(nodeState.state());

                    // iterate over any var-length transitions and add them to the appropriate priority queue at depth +
                    // length
                    for (var mre : nodeState.state().getMultiRelationshipExpansions(direction)) {
                        // here we subtract 1 to account for the initial source/target nodes being discovered at depth 0
                        var depth = foundNodes.depth(direction) - 1 + mre.length();
                        foundNodes.enqueueScheduled(depth, nodeState, mre, direction);
                    }
                }
            }

            hooks.expandNode(dbNodeId, statesList, direction);

            pgCursor.setNodeAndStates(dbNodeId, statesList, direction);
            while (pgCursor.next()) {
                long foundNode = pgCursor.otherNodeReference();
                var re = pgCursor.relationshipExpansion();

                switch (direction) {
                    case FORWARD -> {
                        var nextNode = encounter(foundNode, re.targetState(), direction);
                        var node = statesById.get(re.sourceState().id());

                        var signpost = TwoWaySignpost.fromRelExpansion(
                                mt,
                                node,
                                pgCursor.relationshipReference(),
                                nextNode,
                                re,
                                foundNodes.forwardDepth(),
                                tracker.lengths());

                        if (globalState.searchMode == SearchMode.Unidirectional
                                || !nextNode.hasSourceSignpost(signpost)) {
                            nextNode.addSourceSignpost(signpost, foundNodes.forwardDepth());
                        }
                    }

                    case BACKWARD -> {
                        var nextNode = encounter(foundNode, re.sourceState(), direction);
                        var node = statesById.get(re.targetState().id());

                        var signpost = TwoWaySignpost.fromRelExpansion(
                                mt, nextNode, pgCursor.relationshipReference(), node, re, tracker.lengths());

                        if (!nextNode.hasTargetSignpost(signpost)) {
                            var addedSignpost = node.upsertSourceSignpost(signpost);
                            addedSignpost.setMinTargetDistance(
                                    foundNodes.backwardDepth(), PGPathPropagatingBFS.Phase.Expansion);
                        }
                    }
                }
            }
        }

        // look in the priority queue to see if there are any var-length transitions to expand at this depth.
        // if there are then run DFS on them and add the final node states to the collection
        for (var mre = foundNodes.dequeueScheduled(direction);
                mre != null;
                mre = foundNodes.dequeueScheduled(direction)) {
            multiHopDFS(mre.start(), mre.expansion(), direction);
        }

        foundNodes.commitBuffer(direction);
    }

    public void setTracer(KernelReadTracer tracer) {
        pgCursor.setTracer(tracer);
    }

    @Override
    public void close() throws Exception {
        // globalState is not owned by this class; it should be closed by the consumer
        pgCursor.close();
        statesList.close();
    }

    private static class CachingRelCursor {

        private record CachedNode(long nodeId, int[] types, Direction direction) {
            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                CachedNode that = (CachedNode) o;
                return nodeId == that.nodeId && Arrays.equals(types, that.types) && direction == that.direction;
            }

            @Override
            public int hashCode() {
                return Objects.hash(nodeId, Arrays.hashCode(types), direction);
            }

            public static long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(CachedNode.class);
        }

        private record CachedRel(
                long relationshipReference,
                int type,
                long sourceNodeReference,
                long targetNodeReference,
                long otherNodeReference,
                long originNodeReference)
                implements RelationshipTraversalEntities {
            public static long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(CachedRel.class);
        }

        private final ProductGraphTraversalCursor.DataGraphRelationshipCursor relCursor;
        private final HeapTracking.Map<CachedNode, List<RelationshipTraversalEntities>> cache;
        private final MemoryTracker mt;

        public CachingRelCursor(ProductGraphTraversalCursor.DataGraphRelationshipCursor relCursor, MemoryTracker mt) {
            this.relCursor = relCursor;
            // we do not need to close this collection; the whole of PGPathPropagatingBFS uses a scoped memory tracker
            this.cache = HeapTrackingCollections.newMap(mt);
            this.mt = mt;
        }

        public Iterator<RelationshipTraversalEntities> iterator(long node, int[] types, Direction direction) {
            var cacheKey = new CachedNode(node, types, direction);
            var cached = cache.get(cacheKey);
            if (cached != null) {
                return cached.iterator();
            } else {
                HeapTrackingArrayList<RelationshipTraversalEntities> currentCache =
                        HeapTrackingCollections.newArrayList(this.mt);
                this.mt.allocateHeap(CachedNode.SHALLOW_SIZE);
                cache.put(cacheKey, currentCache);
                relCursor.setNode(node, RelationshipSelection.selection(types, direction));
                return new PrefetchingIterator<>() {
                    @Override
                    protected RelationshipTraversalEntities fetchNextOrNull() {
                        if (relCursor.nextRelationship()) {
                            var cached = new CachedRel(
                                    relCursor.relationshipReference(),
                                    relCursor.type(),
                                    relCursor.sourceNodeReference(),
                                    relCursor.targetNodeReference(),
                                    relCursor.otherNodeReference(),
                                    relCursor.originNodeReference());
                            mt.allocateHeap(CachedRel.SHALLOW_SIZE);
                            currentCache.add(cached);
                            return cached;
                        } else {
                            return null;
                        }
                    }
                };
            }
        }
    }
}
