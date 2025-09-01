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
package org.neo4j.kernel.api.vector;

import org.neo4j.values.AnyValue;

public interface VectorSimilarityFunction {
    String name();

    default float[] maybeToValidVector(AnyValue candidate) {
        return maybeToValidVector(VectorCandidate.maybeFrom(candidate));
    }

    default float[] toValidVector(AnyValue candidate) {
        return toValidVector(VectorCandidate.from(candidate));
    }

    float[] maybeToValidVector(VectorCandidate candidate);

    float[] toValidVector(VectorCandidate candidate);

    float compare(float[] vector1, float[] vector2);
}
