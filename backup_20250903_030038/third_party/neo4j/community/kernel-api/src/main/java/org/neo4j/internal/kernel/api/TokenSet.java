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
package org.neo4j.internal.kernel.api;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_INT_ARRAY;

import java.util.NoSuchElementException;

/**
 * Set of token ids.
 *
 * Modifications are not reflected in the TokenSet and there is no guaranteed
 * order.
 */
public interface TokenSet {
    int numberOfTokens();

    int token(int offset);

    boolean contains(int token);

    int[] all();

    TokenSet NONE = new TokenSet() {
        @Override
        public int numberOfTokens() {
            return 0;
        }

        @Override
        public int token(int offset) {
            throw new NoSuchElementException();
        }

        @Override
        public boolean contains(int token) {
            return false;
        }

        @Override
        public int[] all() {
            return EMPTY_INT_ARRAY;
        }
    };
}
