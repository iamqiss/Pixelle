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
package org.neo4j.internal.schema;

import static org.neo4j.internal.schema.AllIndexProviderDescriptors.RANGE_DESCRIPTOR;

import org.neo4j.common.TokenNameLookup;
import org.neo4j.internal.schema.SchemaCommand.IndexCommand;

public interface SchemaCommandUtils {

    static IndexPrototype forSchema(
            IndexCommand.Create command, SchemaDescriptor schema, IndexProviderDescriptor descriptorProvider) {
        return IndexPrototype.forSchema(schema, descriptorProvider).withIndexType(command.indexType());
    }

    static IndexPrototype backingIndex(SchemaDescriptor schema) {
        return IndexPrototype.uniqueForSchema(schema, RANGE_DESCRIPTOR);
    }

    static IndexPrototype withName(String name, IndexPrototype prototype, TokenNameLookup tokenNameLookup) {
        if (name != null && !name.isEmpty()) {
            return prototype.withName(name);
        }

        return prototype.withName(SchemaNameUtil.generateName(prototype, tokenNameLookup));
    }

    static ConstraintDescriptor withName(
            String name, ConstraintDescriptor constraint, TokenNameLookup tokenNameLookup) {
        if (name != null && !name.isEmpty()) {
            return constraint.withName(name);
        }

        return constraint.withName(SchemaNameUtil.generateName(constraint, tokenNameLookup));
    }
}
