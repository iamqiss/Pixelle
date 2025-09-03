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

import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.internal.schema.ConstraintDescriptor;

public class NodeKeyConstraintDefinition extends NodeConstraintDefinition {
    public NodeKeyConstraintDefinition(
            InternalSchemaActions actions, ConstraintDescriptor constraint, IndexDefinition indexDefinition) {
        super(actions, constraint, indexDefinition);
    }

    @Override
    public ConstraintType getConstraintType() {
        assertInUnterminatedTransaction();
        return ConstraintType.NODE_KEY;
    }

    @Override
    public String toString() {
        final String labelName = escapeName(label.name());
        return format(
                "FOR (%1$s:%2$s) REQUIRE %3$s IS NODE KEY",
                labelName.toLowerCase(), labelName, propertyText(labelName.toLowerCase()));
    }
}
