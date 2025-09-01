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

import java.io.Closeable;
import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.DummyRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.neo4j.batchimport.api.input.IdType;
import org.neo4j.internal.batchimport.input.Groups;

/**
 * There should be a 1:1 match between Reader and file for now.
 */
class ParquetDataReader implements Closeable {

    private final ParquetData parquetDataFile;
    private final Groups groups;
    private final IdType idType;
    private final Supplier<ZoneId> defaultTimezoneSupplier;
    private final String arrayDelimiter;
    private final ParquetFileReader reader;
    private final AtomicInteger blockCounter;
    private final MessageType schema;
    private final GroupConverter recordConverter;
    private final String createdBy;
    private final List<ColumnDescriptor> parquetColumns;

    ParquetDataReader(
            ParquetData parquetDataFile,
            Groups groups,
            IdType idType,
            Supplier<ZoneId> defaultTimezoneSupplier,
            String arrayDelimiter) {
        this.parquetDataFile = parquetDataFile;
        this.groups = groups;
        this.idType = idType;
        this.defaultTimezoneSupplier = defaultTimezoneSupplier;
        this.arrayDelimiter = arrayDelimiter;
        var path = parquetDataFile.file();

        try {
            this.reader = ParquetFileReader.open(
                    ParquetInput.ParquetImportInputFile.of(path),
                    ParquetReadOptions.builder().build());
            var metadata = this.reader.getFileMetaData();
            this.schema = metadata.getSchema();
            this.recordConverter = new DummyRecordConverter(this.schema).getRootConverter();
            this.createdBy = metadata.getCreatedBy();

            var columnsToRead = parquetDataFile.columns().stream()
                    .map(ParquetColumn::columnName)
                    .toList();
            this.parquetColumns = schema.getColumns().stream()
                    .filter(c -> columnsToRead.contains(c.getPath()[0]))
                    .toList();

            this.blockCounter = new AtomicInteger(0);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Iterator<List<Object>> next() throws IOException {
        var nextRowGroupIndex = blockCounter.getAndIncrement();
        if (nextRowGroupIndex >= reader.getRowGroups().size()) {
            return null;
        }

        return new ParquetRowGroupReader(reader.readRowGroup(nextRowGroupIndex));
    }

    public ParquetData getParquetDataFile() {
        return parquetDataFile;
    }

    public Groups getGroups() {
        return groups;
    }

    public IdType getIdType() {
        return idType;
    }

    public Supplier<ZoneId> getDefaultTimezoneSupplier() {
        return defaultTimezoneSupplier;
    }

    public String getArrayDelimiter() {
        return arrayDelimiter;
    }

    private class ParquetRowGroupReader implements Iterator<List<Object>> {
        private final List<ColumnReader> columnReaders;
        private final long rowCount;
        private long rowIndex;

        ParquetRowGroupReader(PageReadStore store) {
            this.rowCount = store.getRowCount();

            var columnReadStore = new ColumnReadStoreImpl(
                    store,
                    ParquetDataReader.this.recordConverter,
                    ParquetDataReader.this.schema,
                    ParquetDataReader.this.createdBy);
            this.columnReaders = parquetColumns.stream()
                    .map(columnReadStore::getColumnReader)
                    .toList();
        }

        @Override
        public boolean hasNext() {
            return rowIndex < rowCount;
        }

        @Override
        public List<Object> next() {
            var result = new ArrayList<>();

            for (ColumnReader columnReader : this.columnReaders) {
                result.add(readValue(columnReader));
                columnReader.consume();
                if (columnReader.getCurrentRepetitionLevel() != 0) {
                    throw new IllegalStateException("Unexpected repetition");
                }
            }

            this.rowIndex++;

            return result;
        }

        private static Object readValue(ColumnReader columnReader) {
            ColumnDescriptor column = columnReader.getDescriptor();
            PrimitiveType primitiveType = column.getPrimitiveType();
            int maxDefinitionLevel = column.getMaxDefinitionLevel();

            if (columnReader.getCurrentDefinitionLevel() == maxDefinitionLevel) {
                return switch (primitiveType.getPrimitiveTypeName()) {
                    case BINARY, FIXED_LEN_BYTE_ARRAY, INT96 -> primitiveType
                            .stringifier()
                            .stringify(columnReader.getBinary());
                    case BOOLEAN -> columnReader.getBoolean();
                    case DOUBLE -> columnReader.getDouble();
                    case FLOAT -> columnReader.getFloat();
                    case INT32 -> columnReader.getInteger();
                    case INT64 -> columnReader.getLong();
                };
            } else {
                return null;
            }
        }
    }

    @Override
    public void close() throws IOException {
        this.reader.close();
    }
}
