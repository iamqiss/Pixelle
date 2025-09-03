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
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.util.Preconditions;

final class NodeLabelExistenceConstraintDefinition extends BaseConstraintDefinition {
    final Label constrainedLabel;
    final Label requiredLabel;

    NodeLabelExistenceConstraintDefinition(
            InternalSchemaActions actions,
            ConstraintDescriptor constraint,
            Label constrainedLabel,
            Label requiredLabel) {
        super(actions, constraint);
        this.constrainedLabel = constrainedLabel;
        this.requiredLabel = requiredLabel;
        Preconditions.checkState(
                constraint.isNodeLabelExistenceConstraint(), "constraint expected to be NodeLabelExistenceConstraint");
    }

    @Override
    public Label getLabel() {
        assertInUnterminatedTransaction();
        return constrainedLabel;
    }

    @Override
    public ConstraintType getConstraintType() {
        return ConstraintType.NODE_LABEL_EXISTENCE;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof NodeLabelExistenceConstraintDefinition that) {
            return this.constrainedLabel.name().equals(that.constrainedLabel.name())
                    && this.requiredLabel.name().equals(that.requiredLabel.name());
        }

        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(constrainedLabel.name(), requiredLabel.name());
    }

    @Override
    public String toString() {
        final String constrainedLabelName = escapeName(constrainedLabel.name());
        final String requiredLabelName = escapeName(requiredLabel.name());
        return format("(:%s => :%s)", constrainedLabelName, requiredLabelName);
    }
}
