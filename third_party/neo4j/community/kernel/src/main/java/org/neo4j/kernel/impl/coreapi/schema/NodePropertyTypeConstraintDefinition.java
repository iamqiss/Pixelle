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

import java.util.Arrays;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.graphdb.schema.PropertyType;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.constraints.PropertyTypeSet;
import org.neo4j.internal.schema.constraints.SchemaValueType;

public class NodePropertyTypeConstraintDefinition extends NodeConstraintDefinition {

    public NodePropertyTypeConstraintDefinition(
            InternalSchemaActions actions, ConstraintDescriptor constraint, Label label, String propertyKey) {
        super(actions, constraint, label, new String[] {propertyKey});
    }

    @Override
    public ConstraintType getConstraintType() {
        assertInUnterminatedTransaction();
        return ConstraintType.NODE_PROPERTY_TYPE;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NodePropertyTypeConstraintDefinition that = (NodePropertyTypeConstraintDefinition) o;
        return label.name().equals(that.label.name())
                && Arrays.equals(propertyKeys, that.propertyKeys)
                // We use this here instead of `getPropertyType()` because that one checks
                // assertInUnterminatedTransaction() and equals/hashCode/toString should still
                // work after a transaction has been terminated.
                && constraint
                        .asPropertyTypeConstraint()
                        .propertyType()
                        .equals(that.constraint.asPropertyTypeConstraint().propertyType());
    }

    @Override
    public int hashCode() {
        int result = 31 * label.name().hashCode();
        result = 31 * result + Arrays.hashCode(propertyKeys);
        // We use this here instead of `getPropertyType()` because that one checks
        // assertInUnterminatedTransaction() and equals/hashCode/toString should still
        // work after a transaction has been terminated.
        result = 31 * result
                + constraint.asPropertyTypeConstraint().propertyType().hashCode();
        return result;
    }

    @Override
    public String toString() {
        final String labelName = escapeName(label.name());
        return format(
                "FOR (%s:%s) REQUIRE %s IS :: %s",
                labelName.toLowerCase(),
                labelName,
                propertyText(labelName.toLowerCase()),
                // We use this here instead of `getPropertyType()` because that one checks
                // assertInUnterminatedTransaction() and equals/hashCode/toString should still
                // work after a transaction has been terminated.
                constraint.asPropertyTypeConstraint().propertyType().userDescription());
    }

    @Override
    public PropertyType[] getPropertyType() {
        assertInUnterminatedTransaction();
        PropertyTypeSet propertyTypeSet = constraint.asPropertyTypeConstraint().propertyType();
        return propertyTypeSet.stream().map(SchemaValueType::toPublicApi).toArray(PropertyType[]::new);
    }
}
