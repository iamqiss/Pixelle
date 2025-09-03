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
package org.neo4j.internal.batchimport.input.parquet;

import java.io.PrintStream;

/**
 * Basically the same as {@link org.neo4j.internal.batchimport.input.csv.CsvInput.Monitor} but since it
 * lives in another package and should not be shared, this is the re-implementation for Parquet.
 */
public class ParquetMonitor {
    private final PrintStream out;

    public ParquetMonitor(PrintStream out) {
        this.out = out;
    }

    public void typeNormalized(String sourceDescription, String name, String fromType, String toType) {
        out.printf(
                "  Property type of '%s' normalized from '%s' --> '%s' in %s%n",
                name, fromType, toType, sourceDescription);
    }

    public void noNodeLabelsSpecified(String sourceDescription) {
        out.printf(
                "WARN: file group with header file %s specifies no node labels, which could be a mistake%n",
                sourceDescription);
    }

    public void noRelationshipTypeSpecified(String sourceDescription) {
        out.printf(
                "WARN: file group with header file %s specifies no relationship type, which could be a mistake%n",
                sourceDescription);
    }
}
