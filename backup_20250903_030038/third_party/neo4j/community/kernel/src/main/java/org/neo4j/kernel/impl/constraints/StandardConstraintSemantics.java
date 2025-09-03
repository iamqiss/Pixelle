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
package org.neo4j.kernel.impl.constraints;

import static org.neo4j.common.EntityType.NODE;
import static org.neo4j.internal.schema.GraphTypeDependence.DEPENDENT;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.RelationshipTypeIndexCursor;
import org.neo4j.internal.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.RelationTypeSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.internal.schema.constraints.KeyConstraintDescriptor;
import org.neo4j.internal.schema.constraints.NodeLabelExistenceConstraintDescriptor;
import org.neo4j.internal.schema.constraints.RelationshipEndpointLabelConstraintDescriptor;
import org.neo4j.internal.schema.constraints.TypeConstraintDescriptor;
import org.neo4j.internal.schema.constraints.UniquenessConstraintDescriptor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.StandardConstraintRuleAccessor;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;

@ServiceProvider
public class StandardConstraintSemantics extends ConstraintSemantics {
    public static final String ERROR_MESSAGE_EXISTS = "Property existence constraint requires Neo4j Enterprise Edition";
    public static final String ERROR_MESSAGE_KEY_SUFFIX = "Key constraint requires Neo4j Enterprise Edition";
    public static final String ERROR_MESSAGE_TYPE = "Property type constraint requires Neo4j Enterprise Edition";
    public static final String ERROR_MESSAGE_RELATIONSHIP_ENDPOINT_LABEL =
            "Relationship endpoint label constraint requires Neo4j Enterprise Edition";
    public static final String ERROR_MESSAGE_NODE_LABEL_EXISTENCE =
            "Node label existence constraint requires Neo4j Enterprise Edition";

    protected final StandardConstraintRuleAccessor accessor = new StandardConstraintRuleAccessor();

    public StandardConstraintSemantics() {
        this(1);
    }

    protected StandardConstraintSemantics(int priority) {
        super(priority);
    }

    @Override
    public String getName() {
        return "standardConstraints";
    }

    @Override
    public void assertKeyConstraintAllowed(SchemaDescriptor descriptor) throws CreateConstraintFailureException {
        throw keyConstraintsNotAllowed(descriptor);
    }

    @Override
    public void validateNodeKeyConstraint(
            NodeLabelIndexCursor allNodes,
            NodeCursor nodeCursor,
            PropertyCursor propertyCursor,
            LabelSchemaDescriptor descriptor,
            TokenNameLookup tokenNameLookup)
            throws CreateConstraintFailureException {
        throw keyConstraintsNotAllowed(descriptor);
    }

    @Override
    public void validateRelKeyConstraint(
            RelationshipTypeIndexCursor allRelationships,
            RelationshipScanCursor relCursor,
            PropertyCursor propertyCursor,
            RelationTypeSchemaDescriptor descriptor,
            TokenNameLookup tokenNameLookup)
            throws CreateConstraintFailureException {
        throw keyConstraintsNotAllowed(descriptor);
    }

    @Override
    public void validateNodePropertyExistenceConstraint(
            NodeLabelIndexCursor allNodes,
            NodeCursor nodeCursor,
            PropertyCursor propertyCursor,
            LabelSchemaDescriptor descriptor,
            TokenNameLookup tokenNameLookup,
            boolean isDependent)
            throws CreateConstraintFailureException {
        throw propertyExistenceConstraintsNotAllowed(descriptor, isDependent);
    }

    @Override
    public void validateRelationshipPropertyExistenceConstraint(
            RelationshipScanCursor relationshipCursor,
            PropertyCursor propertyCursor,
            RelationTypeSchemaDescriptor descriptor,
            TokenNameLookup tokenNameLookup,
            boolean isDependent)
            throws CreateConstraintFailureException {
        throw propertyExistenceConstraintsNotAllowed(descriptor, isDependent);
    }

    @Override
    public void validateRelationshipPropertyExistenceConstraint(
            RelationshipTypeIndexCursor allRelationships,
            PropertyCursor propertyCursor,
            RelationTypeSchemaDescriptor descriptor,
            TokenNameLookup tokenNameLookup,
            boolean isDependent)
            throws CreateConstraintFailureException {
        throw propertyExistenceConstraintsNotAllowed(descriptor, isDependent);
    }

    @Override
    public ConstraintDescriptor readConstraint(ConstraintDescriptor constraint) {
        // Opening a store in Community Edition with Enterprise constraints should not work
        return switch (constraint.type()) {
            case UNIQUE -> constraint;
            case EXISTS -> throw new IllegalStateException(ERROR_MESSAGE_EXISTS);
            case UNIQUE_EXISTS -> throw new IllegalStateException(keyConstraintErrorMessage(constraint.schema()));
            case PROPERTY_TYPE -> throw new IllegalStateException(ERROR_MESSAGE_TYPE);
            case RELATIONSHIP_ENDPOINT_LABEL -> throw new IllegalStateException(
                    ERROR_MESSAGE_RELATIONSHIP_ENDPOINT_LABEL);
            case NODE_LABEL_EXISTENCE -> throw new IllegalStateException(ERROR_MESSAGE_NODE_LABEL_EXISTENCE);
        };
    }

    private static CreateConstraintFailureException propertyExistenceConstraintsNotAllowed(
            SchemaDescriptor descriptor, boolean isDependent) {
        // When creating a Property Existence Constraint in Community Edition
        return new CreateConstraintFailureException(
                ConstraintDescriptorFactory.existsForSchema(descriptor, isDependent), ERROR_MESSAGE_EXISTS);
    }

    private static CreateConstraintFailureException propertyTypeConstraintsNotAllowed(
            TypeConstraintDescriptor descriptor) {
        // When creating a Property Type Constraint in Community Edition
        return new CreateConstraintFailureException(descriptor, ERROR_MESSAGE_TYPE);
    }

    private static CreateConstraintFailureException relationshipEndpointLabelConstraintsNotAllowed(
            RelationshipEndpointLabelConstraintDescriptor descriptor) {
        return new CreateConstraintFailureException(descriptor, ERROR_MESSAGE_RELATIONSHIP_ENDPOINT_LABEL);
    }

    private static CreateConstraintFailureException nodeLabelExistenceConstraintsNotAllowed(
            NodeLabelExistenceConstraintDescriptor descriptor) {
        return new CreateConstraintFailureException(descriptor, ERROR_MESSAGE_NODE_LABEL_EXISTENCE);
    }

    private static String keyConstraintErrorMessage(SchemaDescriptor descriptor) {
        return (descriptor.entityType() == NODE ? "Node " : "Relationship ") + ERROR_MESSAGE_KEY_SUFFIX;
    }

    private static CreateConstraintFailureException keyConstraintsNotAllowed(SchemaDescriptor descriptor) {
        // When creating a Key Constraint in Community Edition
        return new CreateConstraintFailureException(
                ConstraintDescriptorFactory.keyForSchema(descriptor), keyConstraintErrorMessage(descriptor));
    }

    @Override
    public ConstraintDescriptor createUniquenessConstraintRule(
            long ruleId, UniquenessConstraintDescriptor descriptor, long indexId) {
        return accessor.createUniquenessConstraintRule(ruleId, descriptor, indexId);
    }

    @Override
    public ConstraintDescriptor createKeyConstraintRule(long ruleId, KeyConstraintDescriptor descriptor, long indexId)
            throws CreateConstraintFailureException {
        throw keyConstraintsNotAllowed(descriptor.schema());
    }

    @Override
    public ConstraintDescriptor createExistenceConstraint(long ruleId, ConstraintDescriptor descriptor)
            throws CreateConstraintFailureException {
        throw propertyExistenceConstraintsNotAllowed(
                descriptor.schema(), descriptor.graphTypeDependence() == DEPENDENT);
    }

    @Override
    public ConstraintDescriptor createPropertyTypeConstraint(long ruleId, TypeConstraintDescriptor descriptor)
            throws CreateConstraintFailureException {
        throw propertyTypeConstraintsNotAllowed(descriptor);
    }

    @Override
    public ConstraintDescriptor createRelationshipEndpointLabelConstraint(
            long ruleId, RelationshipEndpointLabelConstraintDescriptor descriptor)
            throws CreateConstraintFailureException {
        throw relationshipEndpointLabelConstraintsNotAllowed(descriptor);
    }

    @Override
    public ConstraintDescriptor createNodeLabelExistenceConstraint(
            long ruleId, NodeLabelExistenceConstraintDescriptor descriptor) throws CreateConstraintFailureException {
        throw nodeLabelExistenceConstraintsNotAllowed(descriptor);
    }

    @Override
    public TxStateVisitor decorateTxStateVisitor(
            StorageReader storageReader,
            Read read,
            CursorFactory cursorFactory,
            ReadableTransactionState state,
            TxStateVisitor visitor,
            CursorContext cursorContext,
            MemoryTracker memoryTracker) {
        return visitor;
    }

    @Override
    public void validateNodePropertyExistenceConstraint(
            NodeCursor nodeCursor,
            PropertyCursor propertyCursor,
            LabelSchemaDescriptor descriptor,
            TokenNameLookup tokenNameLookup,
            boolean isDependent)
            throws CreateConstraintFailureException {
        throw propertyExistenceConstraintsNotAllowed(descriptor, isDependent);
    }

    @Override
    public void validateNodeKeyConstraint(
            NodeCursor nodeCursor,
            PropertyCursor propertyCursor,
            LabelSchemaDescriptor descriptor,
            TokenNameLookup tokenNameLookup)
            throws CreateConstraintFailureException {
        throw keyConstraintsNotAllowed(descriptor);
    }

    @Override
    public void validateRelKeyConstraint(
            RelationshipScanCursor relCursor,
            PropertyCursor propertyCursor,
            RelationTypeSchemaDescriptor descriptor,
            TokenNameLookup tokenNameLookup)
            throws CreateConstraintFailureException {
        throw keyConstraintsNotAllowed(descriptor);
    }

    @Override
    public void validateNodePropertyTypeConstraint(
            NodeCursor nodeCursor,
            PropertyCursor propertyCursor,
            TypeConstraintDescriptor descriptor,
            TokenNameLookup tokenNameLookup)
            throws CreateConstraintFailureException {
        throw propertyTypeConstraintsNotAllowed(descriptor);
    }

    @Override
    public void validateNodePropertyTypeConstraint(
            NodeLabelIndexCursor allNodes,
            NodeCursor nodeCursor,
            PropertyCursor propertyCursor,
            TypeConstraintDescriptor descriptor,
            TokenNameLookup tokenNameLookup)
            throws CreateConstraintFailureException {
        throw propertyTypeConstraintsNotAllowed(descriptor);
    }

    @Override
    public void validateRelationshipPropertyTypeConstraint(
            RelationshipScanCursor relationshipCursor,
            PropertyCursor propertyCursor,
            TypeConstraintDescriptor descriptor,
            TokenNameLookup tokenNameLookup)
            throws CreateConstraintFailureException {
        throw propertyTypeConstraintsNotAllowed(descriptor);
    }

    @Override
    public void validateRelationshipPropertyTypeConstraint(
            RelationshipTypeIndexCursor allRelationships,
            PropertyCursor propertyCursor,
            TypeConstraintDescriptor descriptor,
            TokenNameLookup tokenNameLookup)
            throws CreateConstraintFailureException {
        throw propertyTypeConstraintsNotAllowed(descriptor);
    }

    @Override
    public void validateRelationshipEndpointLabelConstraint(
            RelationshipScanCursor relCursor,
            NodeCursor nodeCursor,
            RelationshipEndpointLabelConstraintDescriptor descriptor,
            TokenNameLookup tokenNameLookup)
            throws CreateConstraintFailureException {
        throw relationshipEndpointLabelConstraintsNotAllowed(descriptor);
    }

    @Override
    public void validateRelationshipEndpointLabelConstraint(
            RelationshipTypeIndexCursor relCursor,
            NodeCursor nodeCursor,
            RelationshipEndpointLabelConstraintDescriptor descriptor,
            TokenNameLookup tokenNameLookup)
            throws CreateConstraintFailureException {
        throw relationshipEndpointLabelConstraintsNotAllowed(descriptor);
    }

    @Override
    public void validateNodeLabelExistenceConstraint(
            NodeLabelIndexCursor allNodes,
            NodeCursor nodeCursor,
            NodeLabelExistenceConstraintDescriptor descriptor,
            TokenNameLookup tokenNameLookup)
            throws CreateConstraintFailureException {
        throw nodeLabelExistenceConstraintsNotAllowed(descriptor);
    }

    @Override
    public void validateNodeLabelExistenceConstraint(
            NodeCursor nodeCursor, NodeLabelExistenceConstraintDescriptor descriptor, TokenNameLookup tokenNameLookup)
            throws CreateConstraintFailureException {
        throw nodeLabelExistenceConstraintsNotAllowed(descriptor);
    }
}
