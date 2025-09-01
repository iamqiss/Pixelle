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

import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;
import org.neo4j.batchimport.api.input.IdType;
import org.neo4j.values.storable.Value;

/**
 * Represents the metadata of a Parquet file column.
 * Those metadata _should_ only get created once per file.
 */
record ParquetColumn(
        String columnName,
        String propertyName,
        String groupName,
        ParquetLogicalColumnType logicalColumnType,
        ParquetColumnType columnType,
        boolean isArray,
        String rawConfiguration,
        Map<String, String> configuration) {

    static ParquetColumn from(String columnNameValue, EntityType knownEntityType) {
        var columnName = columnNameValue;
        boolean isArray = hasArrayDefinition(columnNameValue);
        // get rid of the array definition after we looked for its presence
        columnNameValue = columnNameValue.replace("[]", "");

        EnclosureMatch groupNameMatch = extractGroupName(columnNameValue);
        EnclosureMatch configurationMatch = extractConfiguration(columnNameValue);

        columnNameValue = groupNameMatch.removeFrom(columnNameValue);
        columnNameValue =
                configurationMatch.adjustAfterRemovalOf(groupNameMatch).removeFrom(columnNameValue);
        var propertyName = extractPropertyName(columnNameValue);

        var logicalColumnType =
                ParquetLogicalColumnType.resolve(extractLogicalColumnType(columnNameValue), knownEntityType);
        var columnType = ParquetColumnType.resolve(extractColumnType(logicalColumnType, columnNameValue));

        String rawConfiguration = configurationMatch.getMatch();
        Map<String, String> configuration = parseConfiguration(rawConfiguration);
        return new ParquetColumn(
                columnName,
                propertyName,
                groupNameMatch.getMatch(),
                logicalColumnType,
                columnType,
                isArray,
                rawConfiguration,
                configuration);
    }

    private static EnclosureMatch extractConfiguration(String columnNameValue) {
        return EnclosureMatch.parseEnclosure('{', '}', columnNameValue, true);
    }

    private static EnclosureMatch extractGroupName(String columnNameValue) {
        return EnclosureMatch.parseEnclosure('(', ')', columnNameValue, false);
    }

    private static Map<String, String> parseConfiguration(String rawConfiguration) {
        if (rawConfiguration == null) {
            return Collections.emptyMap();
        }
        return Value.parseStringMap(rawConfiguration);
    }

    String idLabel() {
        return configuration.get("label");
    }

    IdType columnIdType() {
        String idTypeValue = configuration.get("id-type");
        if (idTypeValue == null || idTypeValue.isBlank()) {
            return null;
        }
        return switch (idTypeValue.toUpperCase(Locale.ROOT)) {
            case "INT" -> IdType.INTEGER;
            case "STRING" -> IdType.STRING;
            case "ACTUAL" -> IdType.ACTUAL;
            default -> IdType.ACTUAL;
        };
    }

    // todo this and the following method should get merged
    private static String extractLogicalColumnType(String columnNameValue) {
        if (!columnNameValue.contains(":")) {
            return null;
        }
        return columnNameValue.split(":", 2)[1].trim();
    }

    private static String extractColumnType(ParquetLogicalColumnType logicalColumnType, String columnNameValue) {
        // skip column type detection if there is no type definition to see or the logical type
        // is not a property (this includes also ignored fields)
        if (!columnNameValue.contains(":")
                || logicalColumnType
                        != org.neo4j.internal.batchimport.input.parquet.ParquetLogicalColumnType.PROPERTY) {
            return null;
        }
        return columnNameValue.split(":", 2)[1];
    }

    private static String extractPropertyName(String columnNameValue) {
        var columnNameParts = columnNameValue.split(":", 2);
        // never return empty property name
        return columnNameParts[0].isBlank() ? null : columnNameParts[0];
    }

    private static boolean hasArrayDefinition(String columnName) {
        if (!columnName.contains(":")) {
            return columnName.endsWith("[]");
        }
        return columnName.split(":")[1].contains("[]");
    }

    boolean isRaw() {
        return columnType == ParquetColumnType.RAW;
    }

    boolean hasConfiguration() {
        return rawConfiguration() != null;
    }

    ParquetColumn withoutArray() {
        return new ParquetColumn(
                columnName(),
                propertyName(),
                groupName(),
                logicalColumnType(),
                columnType(),
                false,
                rawConfiguration(),
                configuration());
    }

    boolean hasPropertyName() {
        return propertyName != null && !propertyName.isBlank();
    }

    boolean isIdColumn() {
        return logicalColumnType == ParquetLogicalColumnType.ID;
    }

    boolean isLabelColumn() {
        return logicalColumnType == ParquetLogicalColumnType.LABEL;
    }

    boolean isStartId() {
        return logicalColumnType == ParquetLogicalColumnType.START_ID;
    }

    boolean isEndId() {
        return logicalColumnType == ParquetLogicalColumnType.END_ID;
    }

    boolean isType() {
        return logicalColumnType == ParquetLogicalColumnType.TYPE;
    }

    boolean isIgnoredColumn() {
        return logicalColumnType == ParquetLogicalColumnType.IGNORED;
    }

    Supplier<ZoneId> getTimezone(Supplier<ZoneId> defaultTimezoneSupplier) {
        if (!hasConfiguration()) {
            return defaultTimezoneSupplier;
        }
        return () -> {
            String zoneIdValue = configuration.get("timezone");
            if (zoneIdValue == null) {
                return defaultTimezoneSupplier.get();
            }
            return ZoneId.of(zoneIdValue);
        };
    }

    static String getReservedColumns(EntityType entityType) {
        return "Column types: " + Arrays.toString(ParquetColumnType.values()) + ", logical types: "
                + org.neo4j.internal.batchimport.input.parquet.ParquetLogicalColumnType.getReservedColumns(entityType);
    }

    private record EnclosureMatch(char startSymbol, char endSymbol, int startIndex, int endIndex, String parsedMatch) {

        static EnclosureMatch parseEnclosure(char start, char end, String content, boolean includeSymbols) {
            int startIndex = content.indexOf(start + "");
            if (startIndex == -1) {
                return unmatched(start, end);
            }
            int endIndex = content.lastIndexOf(end + "");
            if (endIndex == -1) {
                return unmatched(start, end);
            }
            String match = content.substring(startIndex + 1, endIndex).trim();
            if (!includeSymbols) {
                return new EnclosureMatch(start, end, startIndex, endIndex, match);
            }
            return new EnclosureMatch(start, end, startIndex, endIndex, "%c%s%c".formatted(start, match, end));
        }

        private static EnclosureMatch unmatched(char start, char end) {
            return new EnclosureMatch(start, end, -1, -1, null);
        }

        String removeFrom(String content) {
            if (!matches()) {
                return content;
            }
            String startString = content.substring(0, startIndex);
            if (endIndex == content.length() - 1) {
                return startString;
            }
            return "%s%s".formatted(startString, content.substring(endIndex + 1));
        }

        EnclosureMatch adjustAfterRemovalOf(EnclosureMatch other) {
            if (!this.matches() || !other.matches()) {
                return this;
            }
            var otherMatchLength = other.endIndex - other.startIndex + 1;
            return new EnclosureMatch(
                    this.startSymbol,
                    this.endSymbol,
                    this.startIndex - otherMatchLength,
                    this.endIndex - otherMatchLength,
                    this.parsedMatch);
        }

        String getMatch() {
            return parsedMatch;
        }

        private boolean matches() {
            return parsedMatch != null;
        }
    }
}
