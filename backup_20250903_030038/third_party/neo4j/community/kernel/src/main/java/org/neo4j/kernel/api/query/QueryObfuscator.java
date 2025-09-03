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
package org.neo4j.kernel.api.query;

import java.util.function.Function;
import org.neo4j.graphdb.InputPosition;
import org.neo4j.values.virtual.MapValue;

public interface QueryObfuscator {
    String obfuscateText(String rawQueryText, int preparserOffset);

    Function<InputPosition, InputPosition> obfuscatePosition(String rawQueryText, int preparserOffset);

    MapValue obfuscateParameters(MapValue rawQueryParameters);

    QueryObfuscator PASSTHROUGH = new QueryObfuscator() {

        @Override
        public String obfuscateText(String rawQueryText, int preparserOffset) {
            return rawQueryText;
        }

        @Override
        public Function<InputPosition, InputPosition> obfuscatePosition(String rawQueryText, int preparserOffset) {
            return Function.identity();
        }

        @Override
        public MapValue obfuscateParameters(MapValue rawQueryParameters) {
            return rawQueryParameters;
        }
    };
}
