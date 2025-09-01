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

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.neo4j.batchimport.api.input.IdType;
import org.neo4j.batchimport.api.input.InputEntityVisitor;
import org.neo4j.internal.batchimport.input.Groups;
import org.neo4j.internal.batchimport.input.InputException;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TimeValue;

/**
 * The data chunk to be stuck to a Parquet reader.
 * One chunk equals one file.
 */
class ParquetDataInputChunk implements ParquetInputChunk {
    private ParquetData parquetDataFile;
    private Groups groups;
    private Supplier<ZoneId> defaultTimezoneSupplier;
    private String arrayDelimiter;
    private IdType idType;
    private Iterator<List<Object>> iterator;

    @Override
    public boolean readWith(ParquetDataReader reader) {
        try {
            iterator = reader.next();
            if (iterator == null) {
                return false;
            }

            parquetDataFile = reader.getParquetDataFile();
            groups = reader.getGroups();
            defaultTimezoneSupplier = reader.getDefaultTimezoneSupplier();
            arrayDelimiter = reader.getArrayDelimiter();
            idType = reader.getIdType();

            return true;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {}

    public boolean next(InputEntityVisitor entityToHydrate) throws IOException {
        if (iterator == null || !iterator.hasNext()) {
            return false;
        }

        var columns = parquetDataFile.columns();
        List<Object> readData = iterator.next();
        List<String> labels = new ArrayList<>(filterEmptyLabelsAndTrim(parquetDataFile.labelsOrType()));
        StringBuilder idValue = new StringBuilder();
        Collection<String> typeCandidates = filterEmptyLabelsAndTrim(parquetDataFile.labelsOrType());
        var type = typeCandidates.isEmpty() ? "" : typeCandidates.iterator().next();
        var isRelationshipEntity = false;
        for (int i = 0; i < readData.size(); i++) {
            var parquetColumn = columns.get(i);
            Object readDatum = readData.get(i);
            if (readDatum == null || isEmptyString(readDatum) || parquetColumn.isIgnoredColumn()) {
                continue;
            }
            // node
            if (parquetColumn.isIdColumn()) {
                if (idType == IdType.STRING
                        && (parquetColumn.columnIdType() == IdType.STRING || parquetColumn.columnIdType() == null)) {
                    idValue.append(resolveIdByType(readDatum, null));
                } else if (idType == IdType.INTEGER || parquetColumn.columnIdType() == IdType.INTEGER) {
                    entityToHydrate.id(
                            resolveIdByType(readDatum, parquetColumn.columnIdType()),
                            groups.get(parquetDataFile.groupName()));
                } else {
                    entityToHydrate.id((Long) resolveIdByType(readDatum, parquetColumn.columnIdType()));
                }
                boolean isActualIdColumn = idType == IdType.ACTUAL && parquetColumn.isIdColumn();
                if (!isActualIdColumn && parquetColumn.hasPropertyName()) {
                    entityToHydrate.property(parquetColumn.propertyName(), convertType(readDatum, parquetColumn));
                }
            }
            if (parquetColumn.isLabelColumn()) {
                labels.addAll(readLabelFromEntry(readDatum));
            }
            // common
            if (parquetColumn.hasPropertyName()
                    && parquetColumn.logicalColumnType() == ParquetLogicalColumnType.PROPERTY) {
                entityToHydrate.property(parquetColumn.propertyName(), convertType(readDatum, parquetColumn));
            }
            // relationship
            if (parquetColumn.isStartId()) {
                entityToHydrate.startId(
                        resolveIdByType(readDatum, null), groups.get(parquetDataFile.relationshipStartIdGroupName()));
                isRelationshipEntity = true;
            }
            if (parquetColumn.isEndId()) {
                entityToHydrate.endId(
                        resolveIdByType(readDatum, null), groups.get(parquetDataFile.relationshipEndIdGroupName()));
                isRelationshipEntity = true;
            }
            if (parquetColumn.isType()) {
                if (readDatum instanceof String typeColumnData && !typeColumnData.isBlank()) {
                    type = typeColumnData;
                }
            }
        }
        if (!isRelationshipEntity && !labels.isEmpty()) {
            entityToHydrate.labels(labels.toArray(new String[] {}));
        }
        if (isRelationshipEntity && type != null && !type.isBlank()) {
            entityToHydrate.type(type);
        }
        if (idType == IdType.STRING && !idValue.isEmpty()) {
            entityToHydrate.id(idValue.toString(), groups.get(parquetDataFile.groupName()));
        }
        entityToHydrate.endOfEntity();
        return true;
    }

    private Object convertType(Object object, ParquetColumn parquetColumn) {
        try {
            if (parquetColumn.isRaw()) {
                return object;
            }

            // for now there is only support for String-based arrays
            if (parquetColumn.isArray()) {
                String[] parts = object.toString().split(arrayDelimiter);
                Object[] values = new Object[parts.length];
                ParquetColumn nonArrayType = parquetColumn.withoutArray();
                for (int i = 0; i < parts.length; i++) {
                    values[i] = convertType(parts[i], nonArrayType);
                }
                return values;
            }

            return switch (parquetColumn.columnType()) {
                case POINT -> {
                    if (parquetColumn.hasConfiguration()) {
                        yield PointValue.parse(
                                object.toString(), PointValue.parseHeaderInformation(parquetColumn.rawConfiguration()));
                    }
                    yield PointValue.parse(object.toString());
                }
                case DATE -> DateValue.parse(object.toString());
                case TIME -> TimeValue.parse(
                        object.toString(), parquetColumn.getTimezone(defaultTimezoneSupplier), null);
                case DATE_TIME -> DateTimeValue.parse(
                        object.toString(), parquetColumn.getTimezone(defaultTimezoneSupplier), null);
                case LOCAL_TIME -> LocalTimeValue.parse(object.toString());
                case LOCAL_DATE_TIME -> LocalDateTimeValue.parse(object.toString());
                case DURATION -> DurationValue.parse(object.toString());
                case INT -> Integer.valueOf(object.toString());
                case SHORT -> Short.valueOf(object.toString());
                case STRING -> object.toString();
                case LONG -> Long.valueOf(object.toString());
                case BYTE -> Byte.parseByte(object.toString());
                case DOUBLE -> Double.parseDouble(object.toString());
                case FLOAT -> Float.parseFloat(object.toString());
                default -> object;
            };
        } catch (RuntimeException e) {
            throw new InputException(
                    "could not convert %s to %s".formatted(object.toString(), parquetColumn.columnType()), e);
        }
    }

    private boolean isEmptyString(Object object) {
        return object instanceof String stringValue && stringValue.isEmpty();
    }

    private Object resolveIdByType(Object id, IdType columnIdType) {
        if (id instanceof String stringId) {
            return stringId;
        } else if (id instanceof Long longId) {
            return longId;
        } else if (id instanceof Integer intId) {
            if (columnIdType == IdType.INTEGER) {
                return intId;
            }
            return intId.longValue();
        }

        throw new IllegalArgumentException("Cannot convert id of type " + id.getClass());
    }

    private Collection<String> filterEmptyLabelsAndTrim(Collection<String> labels) {
        return labels.stream().filter(s -> !s.isEmpty()).map(String::trim).collect(Collectors.toSet());
    }

    private Collection<String> readLabelFromEntry(Object readDatum) {
        return filterEmptyLabelsAndTrim(Arrays.asList(readDatum.toString().split(arrayDelimiter)));
    }
}
