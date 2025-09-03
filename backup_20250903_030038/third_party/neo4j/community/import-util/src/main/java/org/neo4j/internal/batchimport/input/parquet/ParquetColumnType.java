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

import java.util.Locale;

/**
 * Common property types of Parquet columns.
 */
public enum ParquetColumnType {
    // common types
    RAW(null, null),
    BOOLEAN("BOOLEAN", null),
    POINT("POINT", null),
    DATE("DATE", null),
    TIME("TIME", null),
    DATE_TIME("DATETIME", null),
    LOCAL_TIME("LOCALTIME", null),
    LOCAL_DATE_TIME("LOCALDATETIME", null),
    DURATION("DURATION", null),
    STRING("STRING", null),
    LONG("LONG", null),
    DOUBLE("DOUBLE", null),
    INT("INT", LONG),
    FLOAT("FLOAT", DOUBLE),
    BYTE("BYTE", LONG),
    SHORT("SHORT", LONG),
    CHAR("CHAR", STRING);

    private final String reservedWord;
    private final ParquetColumnType convertedType;

    ParquetColumnType(String reservedWord, ParquetColumnType convertedType) {
        this.reservedWord = reservedWord;
        this.convertedType = convertedType;
    }

    static ParquetColumnType resolve(String typeName) {
        if (typeName == null) {
            return RAW;
        }
        typeName = typeName.toUpperCase(Locale.ROOT);
        for (ParquetColumnType value : values()) {
            if (typeName.equals(value.reservedWord)) {
                return value;
            }
        }
        throw new IllegalArgumentException("Could not find any type matching: " + typeName);
    }

    boolean needsConversion() {
        return this.convertedType != null;
    }

    ParquetColumnType convertedType() {
        return this.convertedType;
    }
}
