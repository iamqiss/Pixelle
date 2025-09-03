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
package org.neo4j.batchimport.api;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_INT_ARRAY;

import java.io.Closeable;
import java.io.IOException;

/**
 * Used by the {@link BatchImporter} to write index entries as it imports node & relationship entities.
 */
public interface IndexImporter extends Closeable {
    IndexImporter EMPTY_IMPORTER = new EmptyIndexImporter();

    /**
     * @param parallel if {@code true} uses a parallel internal writer, which also means that there
     * can be multiple concurrent writers writing index entries.
     * @return a new {@link Writer} able to write index entries.
     */
    Writer writer(boolean parallel);

    class EmptyIndexImporter implements IndexImporter, Writer {
        @Override
        public void change(long entity, int[] removed, int[] added, boolean logical) {}

        @Override
        public void close() throws IOException {}

        @Override
        public Writer writer(boolean parallel) {
            return this;
        }

        @Override
        public void yield() {}
    }

    interface Writer extends Closeable {
        /**
         * Called by the batch importer for entity that is imported
         * @param entity the id of the entity (node id/relationship id)
         * @param tokens the tokens associated with the entity (labels/relationship types)
         */
        default void add(long entity, int[] tokens) {
            change(entity, EMPTY_INT_ARRAY, tokens, true);
        }

        /**
         * Called by the batch importer for entity that is removed, typically after observing
         * duplicate entities or entities violating constraints.
         * @param entity the id of the entity (node id/relationship id)
         * @param tokens the tokens associated with the entity (labels/relationship types)
         */
        default void remove(long entity, int[] tokens) {
            change(entity, tokens, EMPTY_INT_ARRAY, true);
        }

        /**
         * Called by the batch importer for entity that has changes in its entity tokens
         * @param entity the id of the entity (node id/relationship id)
         * @param beforeTokens if {@code logical == true} then it means labels to remove,
         * else labels before the change.
         * @param afterTokens if {@code logical == true} then it means labels to add,
         * else labels after the change.
         * @param logical if {@code true} interprets before/after tokens as remove/add,
         * else as before/after.
         */
        void change(long entity, int[] beforeTokens, int[] afterTokens, boolean logical);

        void yield();
    }
}
