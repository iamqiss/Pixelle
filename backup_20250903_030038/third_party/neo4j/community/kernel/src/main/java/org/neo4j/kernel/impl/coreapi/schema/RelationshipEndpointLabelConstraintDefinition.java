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
package org.neo4j.kernel.impl.coreapi.schema;

import static java.lang.String.format;
import static org.neo4j.internal.helpers.NameUtil.escapeName;

import java.util.Objects;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.EndpointType;
import org.neo4j.util.Preconditions;

final class RelationshipEndpointLabelConstraintDefinition extends BaseConstraintDefinition {
    final RelationshipType constrainedRelationshipType;
    final Label requiredLabel;
    final EndpointType endpointType;

    RelationshipEndpointLabelConstraintDefinition(
            InternalSchemaActions actions,
            ConstraintDescriptor constraint,
            RelationshipType constrainedRelationshipType,
            Label requiredLabel,
            EndpointType endpointType) {
        super(actions, constraint);
        this.constrainedRelationshipType = constrainedRelationshipType;
        this.requiredLabel = requiredLabel;
        this.endpointType = endpointType;
        Preconditions.checkState(
                constraint.isRelationshipEndpointLabelConstraint(),
                "constraint expected to be RelationshipEndpointLabelConstraint");
    }

    @Override
    public RelationshipType getRelationshipType() {
        assertInUnterminatedTransaction();
        return constrainedRelationshipType;
    }

    @Override
    public ConstraintType getConstraintType() {
        return switch (endpointType) {
            case START -> ConstraintType.RELATIONSHIP_SOURCE_LABEL;
            case END -> ConstraintType.RELATIONSHIP_TARGET_LABEL;
        };
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof RelationshipEndpointLabelConstraintDefinition that) {
            return this.constrainedRelationshipType.name().equals(that.constrainedRelationshipType.name())
                    && this.requiredLabel.name().equals(that.requiredLabel.name())
                    && this.endpointType == that.endpointType;
        }

        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(constrainedRelationshipType.name(), requiredLabel.name(), endpointType);
    }

    @Override
    public String toString() {
        final String constrainedRelationshipTypeName = escapeName(constrainedRelationshipType.name());
        final String requiredLabelName = escapeName(requiredLabel.name());
        return switch (endpointType) {
            case START -> format("(:%s)-[:%s => ]->()", requiredLabelName, constrainedRelationshipTypeName);
            case END -> format("()-[:%s => ]->(:%s)", constrainedRelationshipTypeName, requiredLabelName);
        };
    }
}
