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
package org.neo4j.cypher.internal.runtime;

import java.util.Optional;
import org.eclipse.collections.api.set.primitive.IntSet;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.util.CalledFromGeneratedCode;
import org.neo4j.values.AnyValue;
import org.neo4j.values.ElementIdMapper;
import org.neo4j.values.storable.Value;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;
import org.neo4j.values.virtual.VirtualRelationshipValue;

/**
 * Used to expose db access to expressions
 */
public interface DbAccess extends EntityById {
    Value nodeProperty(
            long node, int property, NodeCursor nodeCursor, PropertyCursor propertyCursor, boolean throwOnDeleted);

    Value[] nodeProperties(long node, int[] properties, NodeCursor nodeCursor, PropertyCursor propertyCursor);

    int[] nodePropertyIds(long node, NodeCursor nodeCursor, PropertyCursor propertyCursor);

    int propertyKey(String name);

    String propertyKeyName(int type);

    int nodeLabel(String name);

    String nodeLabelName(int type);

    int relationshipType(String name);

    String relationshipTypeName(int type);

    boolean nodeHasProperty(long node, int property, NodeCursor nodeCursor, PropertyCursor propertyCursor);

    boolean nodeDeletedInThisTransaction(long id);

    Value relationshipProperty(
            long relationship,
            int property,
            RelationshipScanCursor relationshipScanCursor,
            PropertyCursor propertyCursor,
            boolean throwOnDeleted);

    Value relationshipProperty(
            VirtualRelationshipValue relationship,
            int property,
            RelationshipScanCursor relationshipScanCursor,
            PropertyCursor propertyCursor,
            boolean throwOnDeleted);

    Value[] relationshipProperties(
            long relationship,
            int[] properties,
            RelationshipScanCursor relationshipScanCursor,
            PropertyCursor propertyCursor);

    Value[] relationshipProperties(
            VirtualRelationshipValue relationship,
            int[] properties,
            RelationshipScanCursor relationshipScanCursor,
            PropertyCursor propertyCursor);

    int[] relationshipPropertyIds(
            long relationship, RelationshipScanCursor relationshipScanCursor, PropertyCursor propertyCursor);

    int[] relationshipPropertyIds(
            VirtualRelationshipValue relationship,
            RelationshipScanCursor relationshipScanCursor,
            PropertyCursor propertyCursor);

    boolean relationshipHasProperty(
            long relationship,
            int property,
            RelationshipScanCursor relationshipScanCursor,
            PropertyCursor propertyCursor);

    boolean relationshipDeletedInThisTransaction(long id);

    boolean relationshipHasProperty(
            VirtualRelationshipValue relationship,
            int property,
            RelationshipScanCursor relationshipScanCursor,
            PropertyCursor propertyCursor);

    int nodeGetOutgoingDegreeWithMax(int maxDegree, long node, NodeCursor nodeCursor);

    int nodeGetOutgoingDegreeWithMax(int maxDegree, long node, int relationship, NodeCursor nodeCursor);

    int nodeGetIncomingDegreeWithMax(int maxDegree, long node, NodeCursor nodeCursor);

    int nodeGetIncomingDegreeWithMax(int maxDegree, long node, int relationship, NodeCursor nodeCursor);

    int nodeGetTotalDegreeWithMax(int maxDegree, long node, NodeCursor nodeCursor);

    int nodeGetTotalDegreeWithMax(int maxDegree, long node, int relationship, NodeCursor nodeCursor);

    int nodeGetOutgoingDegree(long node, NodeCursor nodeCursor);

    int nodeGetOutgoingDegree(long node, int relationship, NodeCursor nodeCursor);

    int nodeGetIncomingDegree(long node, NodeCursor nodeCursor);

    int nodeGetIncomingDegree(long node, int relationship, NodeCursor nodeCursor);

    int nodeGetTotalDegree(long node, NodeCursor nodeCursor);

    int nodeGetTotalDegree(long node, int relationship, NodeCursor nodeCursor);

    void singleNode(long id, NodeCursor cursor);

    void singleRelationship(long id, RelationshipScanCursor cursor);

    ListValue getLabelsForNode(long id, NodeCursor nodeCursor);

    AnyValue getTypeForRelationship(long id, RelationshipScanCursor relationshipCursor);

    boolean isLabelSetOnNode(int label, long id, NodeCursor nodeCursor);

    boolean areLabelsSetOnNode(int[] labels, long id, NodeCursor nodeCursor);

    boolean isAnyLabelSetOnNode(int[] labels, long id, NodeCursor nodeCursor);

    boolean isALabelSetOnNode(long id, NodeCursor nodeCursor);

    boolean isTypeSetOnRelationship(int typ, long id, RelationshipScanCursor relationshipCursor);

    boolean areTypesSetOnRelationship(int[] types, long id, RelationshipScanCursor relationshipCursor);

    boolean areTypesSetOnRelationship(
            int[] types, VirtualRelationshipValue obj, RelationshipScanCursor relationshipCursor);

    String getPropertyKeyName(int token);

    MapValue nodeAsMap(
            long id,
            NodeCursor nodeCursor,
            PropertyCursor propertyCursor,
            MapValueBuilder seenProperties,
            IntSet seenPropertyTokens);

    MapValue relationshipAsMap(
            long id,
            RelationshipScanCursor relationshipCursor,
            PropertyCursor propertyCursor,
            MapValueBuilder seenProperties,
            IntSet seenPropertyTokens);

    MapValue relationshipAsMap(
            VirtualRelationshipValue relationship,
            RelationshipScanCursor relationshipCursor,
            PropertyCursor propertyCursor,
            MapValueBuilder seenProperties,
            IntSet seenPropertyTokens);

    Value getTxStateNodePropertyOrNull(long nodeId, int propertyKey);

    Value getTxStateRelationshipPropertyOrNull(long relId, int propertyKey);

    AnyValue callFunction(int id, AnyValue[] args, ProcedureCallContext ctx);

    AnyValue callBuiltInFunction(int id, AnyValue[] args, ProcedureCallContext ctx);

    /**
     * @return `Optional.empty` if TxState has no changes.
     *         `Optional.of(true)` if the property was changed.
     *         `Optional.of(false)` if the property or the entity were deleted in TxState.
     */
    @CalledFromGeneratedCode
    Optional<Boolean> hasTxStatePropertyForCachedNodeProperty(long nodeId, int propertyKeyId);

    /**
     * @return `Optional.empty` if TxState has no changes.
     *         `Optional.of(true)` if the property was changed.
     *         `Optional.of(false)` if the property or the entity were deleted in TxState.
     */
    @CalledFromGeneratedCode
    Optional<Boolean> hasTxStatePropertyForCachedRelationshipProperty(long relId, int propertyKeyId);

    /**
     * Get the node count from the count store
     *
     * @param labelId the label of the nodes to count or -1 for wildcard count
     * @return the number of nodes with the given label in the database
     */
    long nodeCountByCountStore(int labelId);

    /**
     * Get the node count from the count store
     *
     * @param startLabelId the label of the start node or -1 for wildcard count
     * @param typeId the type of the relationship or -1 for wildcard count
     * @param endLabelId the label of the end node or -1 for wildcard count
     * @return the number of relationships with the given start label, type and end label in the database
     */
    long relationshipCountByCountStore(int startLabelId, int typeId, int endLabelId);

    ElementIdMapper elementIdMapper();

    Read dataRead();

    ProcedureCallContext procedureCallContext(int fcnId, MemoryTracker memoryTracker);

    ProcedureCallContext procedureCallContext(int procId, String[] originalFieldNames, MemoryTracker memoryTracker);
}
