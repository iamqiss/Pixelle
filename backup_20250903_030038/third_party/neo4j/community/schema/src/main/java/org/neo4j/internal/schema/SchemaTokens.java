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

import java.util.List;
import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand.Create.NodeExistence;
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand.Create.NodeKey;
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand.Create.NodePropertyType;
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand.Create.NodeUniqueness;
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand.Create.RelationshipExistence;
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand.Create.RelationshipKey;
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand.Create.RelationshipPropertyType;
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand.Create.RelationshipUniqueness;
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.NodeFulltext;
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.NodeLookup;
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.NodePoint;
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.NodeRange;
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.NodeText;
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.NodeVector;
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.RelationshipFulltext;
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.RelationshipLookup;
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.RelationshipPoint;
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.RelationshipRange;
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.RelationshipText;
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.RelationshipVector;

public record SchemaTokens(
        ImmutableSet<String> labels, ImmutableSet<String> relationships, ImmutableSet<String> properties) {
    /**
     * @param commands the schema commands whose tokens should be collected
     * @return all the various tokens required by the provided schema commands
     */
    public static SchemaTokens collect(List<SchemaCommand> commands) {
        final var labels = Sets.mutable.<String>empty();
        final var relationships = Sets.mutable.<String>empty();
        final var properties = Sets.mutable.<String>empty();
        for (var command : commands) {
            // oh for java 21 and switch on types
            if (command instanceof NodeRange indexCommand) {
                labels.add(indexCommand.label());
                properties.addAll(indexCommand.properties());
            } else if (command instanceof RelationshipRange indexCommand) {
                relationships.add(indexCommand.type());
                properties.addAll(indexCommand.properties());
            } else if (command instanceof NodeText indexCommand) {
                labels.add(indexCommand.label());
                properties.add(indexCommand.property());
            } else if (command instanceof RelationshipText indexCommand) {
                relationships.add(indexCommand.type());
                properties.add(indexCommand.property());
            } else if (command instanceof NodePoint indexCommand) {
                labels.add(indexCommand.label());
                properties.add(indexCommand.property());
            } else if (command instanceof RelationshipPoint indexCommand) {
                relationships.add(indexCommand.type());
                properties.add(indexCommand.property());
            } else if (command instanceof NodeFulltext indexCommand) {
                labels.addAll(indexCommand.labels());
                properties.addAll(indexCommand.properties());
            } else if (command instanceof RelationshipFulltext indexCommand) {
                relationships.addAll(indexCommand.types());
                properties.addAll(indexCommand.properties());
            } else if (command instanceof NodeVector indexCommand) {
                labels.add(indexCommand.label());
                properties.add(indexCommand.property());
            } else if (command instanceof RelationshipVector indexCommand) {
                relationships.add(indexCommand.type());
                properties.add(indexCommand.property());
            } else if (command instanceof NodeKey constraintCommand) {
                labels.add(constraintCommand.label());
                properties.addAll(constraintCommand.properties());
            } else if (command instanceof RelationshipKey constraintCommand) {
                relationships.add(constraintCommand.type());
                properties.addAll(constraintCommand.properties());
            } else if (command instanceof NodeUniqueness constraintCommand) {
                labels.add(constraintCommand.label());
                properties.addAll(constraintCommand.properties());
            } else if (command instanceof RelationshipUniqueness constraintCommand) {
                relationships.add(constraintCommand.type());
                properties.addAll(constraintCommand.properties());
            } else if (command instanceof NodeExistence constraintCommand) {
                labels.add(constraintCommand.label());
                properties.add(constraintCommand.property());
            } else if (command instanceof RelationshipExistence constraintCommand) {
                relationships.add(constraintCommand.type());
                properties.add(constraintCommand.property());
            } else if (command instanceof NodePropertyType constraintCommand) {
                labels.add(constraintCommand.label());
                properties.add(constraintCommand.property());
            } else if (command instanceof RelationshipPropertyType constraintCommand) {
                relationships.add(constraintCommand.type());
                properties.add(constraintCommand.property());
            } else if (command instanceof NodeLookup || command instanceof RelationshipLookup) {
                // these have no tokens to collect
            } else {
                throw new IllegalStateException(
                        "Unrecognised command - unable to collect schema tokens for: " + command);
            }
        }

        return new SchemaTokens(labels.toImmutable(), relationships.toImmutable(), properties.toImmutable());
    }
}
