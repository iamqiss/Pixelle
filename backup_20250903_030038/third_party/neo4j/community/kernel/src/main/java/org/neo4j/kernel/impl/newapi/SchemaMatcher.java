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
package org.neo4j.kernel.impl.newapi;

import static org.neo4j.collection.PrimitiveArrays.contains;

import java.util.Iterator;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.internal.schema.FulltextSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptorSupplier;
import org.neo4j.storageengine.api.txstate.TransactionStateBehaviour;

/**
 * This class holds functionality to match {@link SchemaDescriptor} to entities.
 */
public class SchemaMatcher {
    private SchemaMatcher() {
        throw new AssertionError("no instance");
    }

    /**
     * Iterate over some schema suppliers, and invoke a callback for every supplier that matches the entity. To match the
     * entity E the supplier must supply a {@link SchemaDescriptor} D, such that E has values for all the properties of D.
     * The supplied schemas are all assumed to match E on token (label or relationship).
     * <p>
     * To avoid unnecessary store lookups, this implementation only gets propertyKeyIds for the entity if some
     * descriptor has a valid token.
     *
     * @param <SUPPLIER> the type to match. Must implement SchemaDescriptorSupplier
     * @param <EXCEPTION> The type of exception that can be thrown when taking the action
     * @param schemaSuppliers The suppliers to match
     * @param specialPropertyId This property id will always count as a match for the descriptor, regardless of
     * whether the entity has this property or not
     * @param stateBehaviour transaction state behaviour flags
     * @param callback The action to take on match
     * @throws EXCEPTION This exception is propagated from the action
     */
    static <SUPPLIER extends SchemaDescriptorSupplier, EXCEPTION extends Exception> void onMatchingSchema(
            Iterator<SUPPLIER> schemaSuppliers,
            int specialPropertyId,
            int[] existingPropertyIds,
            TransactionStateBehaviour stateBehaviour,
            ThrowingConsumer<SUPPLIER, EXCEPTION> callback)
            throws EXCEPTION {
        while (schemaSuppliers.hasNext()) {
            SUPPLIER schemaSupplier = schemaSuppliers.next();
            SchemaDescriptor schema = schemaSupplier.schema();
            if (stateBehaviour.useIndexCommands() && schema.isSchemaDescriptorType(FulltextSchemaDescriptor.class)) {
                if (contains(schema.getPropertyIds(), specialPropertyId) || specialPropertyId < 0) {
                    callback.accept(schemaSupplier);
                }
            } else {
                if (entityHasSchemaProperties(existingPropertyIds, schema.getPropertyIds(), specialPropertyId)) {
                    callback.accept(schemaSupplier);
                }
            }
        }
    }

    private static boolean entityHasSchemaProperties(
            int[] existingPropertyIds, int[] indexPropertyIds, int changedPropertyId) {
        for (int indexPropertyId : indexPropertyIds) {
            if (indexPropertyId != changedPropertyId && !contains(existingPropertyIds, indexPropertyId)) {
                return false;
            }
        }
        return true;
    }
}
