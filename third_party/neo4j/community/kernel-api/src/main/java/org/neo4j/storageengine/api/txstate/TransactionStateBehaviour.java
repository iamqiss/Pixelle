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
package org.neo4j.storageengine.api.txstate;

public interface TransactionStateBehaviour {
    TransactionStateBehaviour DEFAULT_BEHAVIOUR = new TransactionStateBehaviour() {
        @Override
        public boolean keepMetaDataForDeletedRelationship() {
            return false;
        }

        @Override
        public boolean useIndexCommands() {
            return false;
        }
    };

    /**
     * @return whether or not meta data about relationships is kept for deleted relationships.
     * If {@code true} then the memory overhead of deleting relationships will be larger, with the benefit of
     * not having to look up that information during commit.
     */
    boolean keepMetaDataForDeletedRelationship();

    /**
     * @return whether or not transaction state use index commands
     */
    boolean useIndexCommands();
}
