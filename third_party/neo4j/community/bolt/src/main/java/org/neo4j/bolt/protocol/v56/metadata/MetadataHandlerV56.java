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
package org.neo4j.bolt.protocol.v56.metadata;

import org.neo4j.bolt.protocol.common.fsm.response.MetadataConsumer;
import org.neo4j.bolt.protocol.common.fsm.response.metadata.DefaultMetadataHandler;

public final class MetadataHandlerV56 extends DefaultMetadataHandler {
    private static final MetadataHandlerV56 INSTANCE = new MetadataHandlerV56();

    private MetadataHandlerV56() {}

    public static MetadataHandlerV56 getInstance() {
        return INSTANCE;
    }

    @Override
    public void onTransactionDatabase(MetadataConsumer handler, String name) {
        // not available pre Bolt V58
    }
}
