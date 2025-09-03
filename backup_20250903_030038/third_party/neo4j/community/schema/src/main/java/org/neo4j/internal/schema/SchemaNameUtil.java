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

import java.util.Optional;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.hashing.HashFunction;
import org.neo4j.util.Preconditions;

public class SchemaNameUtil {
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public static String sanitiseName(Optional<String> name) {
        if (name.isPresent()) {
            return sanitiseName(name.get());
        }
        throw new IllegalArgumentException("Schema rules must have names.");
    }

    public static String sanitiseName(String name) {
        if (name == null) {
            throw new IllegalArgumentException("Schema rule name cannot be null.");
        }
        name = name.trim();
        if (name.isEmpty() || name.isBlank()) {
            throw new IllegalArgumentException(
                    "Schema rule name cannot be the empty string or only contain whitespace.");
        } else {
            int length = name.length();
            for (int i = 0; i < length; i++) {
                char ch = name.charAt(i);
                if (ch == '\0') {
                    throw new IllegalArgumentException(
                            "Schema rule names are not allowed to contain null-bytes: '" + name + "'.");
                }
            }
        }
        if (ReservedSchemaRuleNames.contains(name)) {
            throw new IllegalArgumentException("The index name '" + name + "' is reserved, and cannot be used. "
                    + "The reserved names are " + ReservedSchemaRuleNames.getReservedNames() + ".");
        }
        return name;
    }

    /**
     * Generate a <em>deterministic</em> name for the given {@link SchemaDescriptorSupplier}.
     * Only {@link SchemaRule} implementations, and {@link IndexPrototype}, are supported arguments for the schema descriptor supplier.
     * This only accepts schemas that does not target any entity tokens or properties. Otherwise, use
     * {@link #generateName(SchemaDescriptorSupplier, TokenNameLookup)}.
     *
     * @param rule The {@link SchemaDescriptorSupplier} to generate a name for.
     * @return A name.
     */
    public static String generateName(SchemaDescriptorSupplier rule) {
        Preconditions.checkArgument(
                rule.schema().getEntityTokenIds().length == 0,
                "Schema should target no entity tokens (labels, relationship types).");
        Preconditions.checkArgument(
                rule.schema().getPropertyIds().length == 0, "Schema should target no property keys.");
        return generateName(rule, null);
    }

    /**
     * Generate a <em>deterministic</em> name for the given {@link SchemaDescriptorSupplier}.
     * Only {@link SchemaRule} implementations, and {@link IndexPrototype}, are supported arguments for the schema descriptor supplier.
     *
     * @param rule The {@link SchemaDescriptorSupplier} to generate a name for.
     * @param tokenNameLookup The {@link TokenNameLookup} to fetch the names for tokens.
     * @return A name.
     */
    public static String generateName(SchemaDescriptorSupplier rule, TokenNameLookup tokenNameLookup) {
        // NOTE to future maintainers: You probably want to avoid touching this function.
        // Last time this was changed, we had some 400+ tests to update.
        HashFunction hf = HashFunction.incrementalXXH64();
        long key = hf.initialise(Boolean.hashCode(rule instanceof ConstraintDescriptor));
        key = hf.update(key, rule.schema().entityType().ordinal());
        key = hf.update(key, rule.schema().schemaPatternMatchingType().ordinal());
        switch (rule.schema().entityType()) {
            case NODE -> key = hf.updateWithArray(key, rule.schema().getEntityTokenIds(), id -> tokenNameLookup
                    .labelGetName(id)
                    .hashCode());
            case RELATIONSHIP -> key = hf.updateWithArray(key, rule.schema().getEntityTokenIds(), id -> tokenNameLookup
                    .relationshipTypeGetName(id)
                    .hashCode());
        }
        key = hf.updateWithArray(key, rule.schema().getPropertyIds(), id -> tokenNameLookup
                .propertyKeyGetName(id)
                .hashCode());

        if (rule instanceof IndexRef<?> indexRef) {
            key = hf.update(key, indexRef.getIndexType().getTypeNumber());
            key = hf.update(key, Boolean.hashCode(indexRef.isUnique()));
            return String.format("index_%x", hf.toInt(hf.finalise(key)));
        }
        if (rule instanceof ConstraintDescriptor constraint) {
            key = hf.update(key, constraint.type().ordinal());
            if (constraint.isIndexBackedConstraint()) {
                key = hf.update(
                        key, constraint.asIndexBackedConstraint().indexType().getTypeNumber());
            }
            if (constraint.enforcesPropertyType()) {
                key = hf.update(
                        key,
                        constraint.asPropertyTypeConstraint().propertyType().hashCode());
            }
            if (constraint.isRelationshipEndpointLabelConstraint()) {
                var relEndpointLabelConstraint = constraint.asRelationshipEndpointLabelConstraint();
                key = hf.update(
                        key,
                        tokenNameLookup
                                .labelGetName(relEndpointLabelConstraint.endpointLabelId())
                                .hashCode());
                key = hf.update(key, relEndpointLabelConstraint.endpointType().ordinal());
            }
            if (constraint.isNodeLabelExistenceConstraint()) {
                key = hf.update(
                        key,
                        tokenNameLookup
                                .labelGetName(constraint
                                        .asNodeLabelExistenceConstraint()
                                        .requiredLabelId())
                                .hashCode());
            }
            return String.format("constraint_%x", hf.toInt(hf.finalise(key)));
        }
        throw new IllegalArgumentException(
                "Don't know how to generate a name for this SchemaDescriptorSupplier implementation: " + rule + ".");
    }
}
