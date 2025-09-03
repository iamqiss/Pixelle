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
package org.neo4j.csv.reader;

/**
 * A chunker that finds the position of the last new line character to determine the row boundary.
 */
public class ClosestNewLineChunker extends NewLineChunker {

    public ClosestNewLineChunker(CharReadable reader, int chunkSize, HeaderSkipper headerSkip) {
        super(reader, chunkSize, headerSkip);
    }

    @Override
    protected int offsetOfLastRow(char[] buffer) {
        for (var i = buffer.length - 1; i >= 0; i--) {
            if (buffer[i] == '\n') {
                return i;
            }
        }
        // There was no newline character, isn't that weird?
        throw new IllegalStateException(
                "Weird input data, no newline character in the whole buffer " + chunkSize + ", not supported a.t.m.");
    }
}
