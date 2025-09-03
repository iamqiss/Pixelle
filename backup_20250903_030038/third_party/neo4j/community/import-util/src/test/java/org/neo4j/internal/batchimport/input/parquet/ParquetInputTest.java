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

import static java.lang.String.format;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.batchimport.api.input.Collector.EMPTY;
import static org.neo4j.batchimport.api.input.IdType.ACTUAL;
import static org.neo4j.batchimport.api.input.IdType.INTEGER;
import static org.neo4j.batchimport.api.input.IdType.STRING;
import static org.neo4j.internal.helpers.ArrayUtil.union;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;

import blue.strategic.parquet.ParquetWriter;
import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Array;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.batchimport.api.InputIterator;
import org.neo4j.batchimport.api.input.Collector;
import org.neo4j.batchimport.api.input.Group;
import org.neo4j.batchimport.api.input.IdType;
import org.neo4j.batchimport.api.input.Input;
import org.neo4j.batchimport.api.input.InputChunk;
import org.neo4j.internal.batchimport.input.Groups;
import org.neo4j.internal.batchimport.input.InputEntity;
import org.neo4j.internal.batchimport.input.InputException;
import org.neo4j.internal.helpers.collection.MapUtil;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.token.CreatingTokenHolder;
import org.neo4j.token.ReadOnlyTokenCreator;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.NamedToken;
import org.neo4j.token.api.TokenHolder;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Values;

@TestDirectoryExtension
@ExtendWith(RandomExtension.class)
class ParquetInputTest {
    @Inject
    private RandomSupport random;

    @Inject
    private TestDirectory directory;

    private final InputEntity visitor = new InputEntity();
    private final Groups groups = new Groups();
    private final Group globalGroup = groups.getOrCreate(null);
    private InputChunk chunk;
    private InputIterator referenceData;
    private int fileCounter;

    private static final ParquetMonitor MONITOR = new ParquetMonitor(System.out);

    @AfterEach
    void cleanup() throws IOException {
        fileCounter = 0;
        directory.cleanup();
    }

    @Test
    void shouldProvideNodesFromParquetInput() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT64).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":LABEL")),
                List.<Object[]>of(new Object[] {123L, "Mattias Persson", "HACKER"}));
        Input input = new ParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, ';', groups, MONITOR);
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 123L, properties("name", "Mattias Persson"), labels("HACKER"));
            assertFalse(chunk.next(visitor));
        }
    }

    @Test
    void shouldProvideRelationshipsFromParquetInput() throws Exception {
        // GIVEN
        Path relationshipFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":START_ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":END_ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":TYPE"),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT64).named("since")),
                List.of(
                        new Object[] {"node1", "node2", "KNOWS", 1234567L},
                        new Object[] {"node2", "node10", "HACKS", 987654L}));
        Input input = new ParquetInput(
                Map.of(), Map.of("", List.<Path[]>of(new Path[] {relationshipFile})), STRING, ';', groups, MONITOR);
        // WHEN/THEN
        try (InputIterator relationships = input.relationships(EMPTY).iterator()) {
            assertNextRelationship(relationships, "node1", "node2", "KNOWS", properties("since", 1234567L));
            assertNextRelationship(relationships, "node2", "node10", "HACKS", properties("since", 987654L));
        }
    }

    @Test
    void shouldHandleMultipleInputGroups() throws Exception {
        // GIVEN multiple input groups, each with their own, specific, header
        Path nodeFile1 = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("kills"),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("health")),
                List.of(new Object[] {"1", "Jim", 10, 100}, new Object[] {"2", "Abathur", 0, 200}));
        Path nodeFile2 = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("type")),
                List.of(new Object[] {"3", "zergling"}, new Object[] {"4", "csv"}));
        Input input = new ParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile1, nodeFile2})),
                Map.of(),
                STRING,
                ';',
                groups,
                MONITOR);
        // WHEN iterating over them, THEN the expected data should come out
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, "1", properties("name", "Jim", "kills", 10, "health", 100), labels());
            assertNextNode(nodes, "2", properties("name", "Abathur", "kills", 0, "health", 200), labels());
            assertNextNode(nodes, "3", properties("type", "zergling"), labels());
            assertNextNode(nodes, "4", properties("type", "csv"), labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldProvideAdditiveLabels() throws Exception {
        // GIVEN
        String[] addedLabels = {"Two", "AddTwo"};
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":LABEL")),
                List.of(new Object[] {0, "First", ""}, new Object[] {1, "Second", "One"}, new Object[] {
                    2, "Third", "One;Two"
                }));
        Input input = new ParquetInput(
                Map.of(Set.of(addedLabels), List.<Path[]>of(new Path[] {nodeFile})),
                Map.of(),
                INTEGER,
                ';',
                groups,
                MONITOR);
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 0L, properties("name", "First"), labels(addedLabels));
            assertNextNode(nodes, 1L, properties("name", "Second"), labels(union(new String[] {"One"}, addedLabels)));
            assertNextNode(nodes, 2L, properties("name", "Third"), labels(union(new String[] {"One"}, addedLabels)));
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldProvideDefaultRelationshipType() throws Exception {
        // GIVEN
        String defaultType = "DEFAULT";
        String customType = "CUSTOM";
        Path relationshipFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":START_ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":END_ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":TYPE")),
                List.of(new Object[] {0, 1, ""}, new Object[] {1, 2, customType}, new Object[] {2, 1, defaultType}));
        Input input = new ParquetInput(
                Map.of(),
                Map.of(defaultType, List.<Path[]>of(new Path[] {relationshipFile})),
                INTEGER,
                ';',
                groups,
                MONITOR);

        // WHEN/THEN
        try (InputIterator relationships = input.relationships(EMPTY).iterator()) {
            assertNextRelationship(relationships, 0L, 1L, defaultType, emptyMap());
            assertNextRelationship(relationships, 1L, 2L, customType, emptyMap());
            assertNextRelationship(relationships, 2L, 1L, defaultType, emptyMap());
            assertFalse(readNext(relationships));
        }
    }

    @Test
    void shouldAllowNodesWithoutIdHeader() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("level")),
                List.of(new Object[] {"Mattias", 1}, new Object[] {"Johan", 2}));
        Input input = new ParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), STRING, ';', groups, MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(nodes, null, null, properties("name", "Mattias", "level", 1), labels());
            assertNextNode(nodes, null, null, properties("name", "Johan", "level", 2), labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldAllowSomeNodesToBeAnonymous() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("level")),
                List.of(new Object[] {"abc", "Mattias", 1}, new Object[] {null, "Johan", 2}));
        Input input = new ParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), STRING, ';', groups, MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(nodes, "abc", properties("name", "Mattias", "level", 1), labels());
            assertNextNode(nodes, null, null, properties("name", "Johan", "level", 2), labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldAllowNodesToBeAnonymousEvenIfIdHeaderIsNamed() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("id:ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("level")),
                List.of(new Object[] {"abc", "Mattias", 1}, new Object[] {null, "Johan", 2}));
        Input input = new ParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), STRING, ';', groups, MONITOR);

        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(nodes, "abc", properties("id", "abc", "name", "Mattias", "level", 1), labels());
            assertNextNode(nodes, null, null, properties("name", "Johan", "level", 2), labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldNotHaveIdSetAsPropertyIfIdHeaderEntryIsNamedForActualIds() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("myId:ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("level")),
                List.of(new Object[] {0, "Mattias", 1}, new Object[] {1, "Johan", 2}));
        Input input = new ParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), ACTUAL, ';', groups, MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(nodes, null, 0L, properties("name", "Mattias", "level", 1), labels());
            assertNextNode(nodes, null, 1L, properties("name", "Johan", "level", 2), labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldIgnoreNullPropertyValues() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("extra")),
                List.of(new Object[] {0, "Mattias", null}, new Object[] {1, "Johan", "Additional"}));
        Input input = new ParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, ';', groups, MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(nodes, 0L, properties("name", "Mattias"), labels());
            assertNextNode(nodes, 1L, properties("name", "Johan", "extra", "Additional"), labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldIgnoreEmptyPropertyValues() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("extra")),
                List.of(new Object[] {0, "Mattias", ""}, new Object[] {1, "Johan", "Additional"}));
        Input input = new ParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, ';', groups, MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(nodes, 0L, properties("name", "Mattias"), labels());
            assertNextNode(nodes, 1L, properties("name", "Johan", "extra", "Additional"), labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldParsePointPropertyValues() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("point:Point")),
                List.of(
                        new Object[] {0, "Mattias", "{x: 2.7, y:3.2 }"},
                        new Object[] {1, "Johan", " { height :0.01 ,longitude:5, latitude : -4.2 } "}));
        Input input = new ParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, ';', groups, MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(
                    nodes,
                    0L,
                    properties(
                            "name",
                            "Mattias",
                            "point",
                            Values.pointValue(CoordinateReferenceSystem.CARTESIAN, 2.7, 3.2)),
                    labels());
            assertNextNode(
                    nodes,
                    1L,
                    properties(
                            "name",
                            "Johan",
                            "point",
                            Values.pointValue(CoordinateReferenceSystem.WGS_84_3D, 5, -4.2, 0.01)),
                    labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldNotParsePointPropertyValuesWithDuplicateKeys() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("point:Point")),
                List.<Object[]>of(
                        new Object[] {0, "Johan", " { height :0.01 ,longitude:5, latitude : -4.2, latitude : 4.2 } "}));
        Input input = new ParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, ';', groups, MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            readNext(nodes);
            fail("Should have failed when key assigned multiple times, but didn't.");
        } catch (InputException ignore) {
            // this is fine
        }
    }

    @Test
    void shouldParsePointPropertyValuesWithCRSInHeader() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("point:Point{crs:WGS-84-3D}")),
                List.<Object[]>of(new Object[] {0, "Johan", " { height :0.01 ,longitude:5, latitude : -4.2 } "}));
        Input input = new ParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, ';', groups, MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(
                    nodes,
                    0L,
                    properties(
                            "name",
                            "Johan",
                            "point",
                            Values.pointValue(CoordinateReferenceSystem.WGS_84_3D, 5, -4.2, 0.01)),
                    labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldUseHeaderInformationToParsePoint() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("point:Point{crs:WGS-84}")),
                List.<Object[]>of(new Object[] {0, "Johan", " { x :1 ,y:2 } "}));
        Input input = new ParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, ';', groups, MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(
                    nodes,
                    0L,
                    properties("name", "Johan", "point", Values.pointValue(CoordinateReferenceSystem.WGS_84, 1, 2)),
                    labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldParseDatePropertyValues() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("date:Date")),
                List.of(new Object[] {0, "Mattias", "2018-02-27"}, new Object[] {1, "Johan", "2018-03-01"}));
        Input input = new ParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, ';', groups, MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(nodes, 0L, properties("name", "Mattias", "date", DateValue.date(2018, 2, 27)), labels());
            assertNextNode(nodes, 1L, properties("name", "Johan", "date", DateValue.date(2018, 3, 1)), labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldParseTimePropertyValues() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("time:Time")),
                List.of(new Object[] {0, "Mattias", "13:37"}, new Object[] {1, "Johan", "16:20:01"}, new Object[] {
                    2, "Bob", "07:30-05:00"
                }));
        Input input = new ParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, ';', groups, MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(
                    nodes, 0L, properties("name", "Mattias", "time", TimeValue.time(13, 37, 0, 0, "+00:00")), labels());
            assertNextNode(
                    nodes, 1L, properties("name", "Johan", "time", TimeValue.time(16, 20, 1, 0, "+00:00")), labels());
            assertNextNode(
                    nodes, 2L, properties("name", "Bob", "time", TimeValue.time(7, 30, 0, 0, "-05:00")), labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldParseTimePropertyValuesWithTimezoneInHeader() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("time:Time{timezone:+02:00}")),
                List.of(new Object[] {0, "Mattias", "13:37"}, new Object[] {1, "Johan", "16:20:01"}, new Object[] {
                    2, "Bob", "07:30-05:00"
                }));
        Input input = new ParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, ';', groups, MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(
                    nodes, 0L, properties("name", "Mattias", "time", TimeValue.time(13, 37, 0, 0, "+02:00")), labels());
            assertNextNode(
                    nodes, 1L, properties("name", "Johan", "time", TimeValue.time(16, 20, 1, 0, "+02:00")), labels());
            assertNextNode(
                    nodes, 2L, properties("name", "Bob", "time", TimeValue.time(7, 30, 0, 0, "-05:00")), labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldParseDateTimePropertyValues() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("time:DateTime")),
                List.of(
                        new Object[] {0, "Mattias", "2018-02-27T13:37"},
                        new Object[] {1, "Johan", "2018-03-01T16:20:01"},
                        new Object[] {2, "Bob", "1981-05-11T07:30-05:00"}));
        Input input = new ParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, ';', groups, MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(
                    nodes,
                    0L,
                    properties("name", "Mattias", "time", DateTimeValue.datetime(2018, 2, 27, 13, 37, 0, 0, "+00:00")),
                    labels());
            assertNextNode(
                    nodes,
                    1L,
                    properties("name", "Johan", "time", DateTimeValue.datetime(2018, 3, 1, 16, 20, 1, 0, "+00:00")),
                    labels());
            assertNextNode(
                    nodes,
                    2L,
                    properties("name", "Bob", "time", DateTimeValue.datetime(1981, 5, 11, 7, 30, 0, 0, "-05:00")),
                    labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldParseDateTimePropertyValuesWithTimezoneInHeader() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("time:DateTime{timezone:Europe/Stockholm}")),
                List.of(
                        new Object[] {0, "Mattias", "2018-02-27T13:37"},
                        new Object[] {1, "Johan", "2018-03-01T16:20:01"},
                        new Object[] {2, "Bob", "1981-05-11T07:30-05:00"}));
        Input input = new ParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, ';', groups, MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(
                    nodes,
                    0L,
                    properties(
                            "name",
                            "Mattias",
                            "time",
                            DateTimeValue.datetime(2018, 2, 27, 13, 37, 0, 0, "Europe/Stockholm")),
                    labels());
            assertNextNode(
                    nodes,
                    1L,
                    properties(
                            "name",
                            "Johan",
                            "time",
                            DateTimeValue.datetime(2018, 3, 1, 16, 20, 1, 0, "Europe/Stockholm")),
                    labels());
            assertNextNode(
                    nodes,
                    2L,
                    properties("name", "Bob", "time", DateTimeValue.datetime(1981, 5, 11, 7, 30, 0, 0, "-05:00")),
                    labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldParseLocalTimePropertyValues() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("time:LocalTime")),
                List.of(new Object[] {0, "Mattias", "13:37"}, new Object[] {1, "Johan", "16:20:01"}));
        Input input = new ParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, ';', groups, MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(
                    nodes, 0L, properties("name", "Mattias", "time", LocalTimeValue.localTime(13, 37, 0, 0)), labels());
            assertNextNode(
                    nodes, 1L, properties("name", "Johan", "time", LocalTimeValue.localTime(16, 20, 1, 0)), labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldParseLocalDateTimePropertyValues() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("time:LocalDateTime")),
                List.of(new Object[] {0, "Mattias", "2018-02-27T13:37"}, new Object[] {1, "Johan", "2018-03-01T16:20:01"
                }));
        Input input = new ParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, ';', groups, MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(
                    nodes,
                    0L,
                    properties("name", "Mattias", "time", LocalDateTimeValue.localDateTime(2018, 2, 27, 13, 37, 0, 0)),
                    labels());
            assertNextNode(
                    nodes,
                    1L,
                    properties("name", "Johan", "time", LocalDateTimeValue.localDateTime(2018, 3, 1, 16, 20, 1, 0)),
                    labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldParseDurationPropertyValues() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("duration:Duration")),
                List.of(new Object[] {0, "Mattias", "P3MT13H37M"}, new Object[] {1, "Johan", "P-1YT4H20M"}));
        Input input = new ParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, ';', groups, MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(
                    nodes,
                    0L,
                    properties("name", "Mattias", "duration", DurationValue.duration(3, 0, 13 * 3600 + 37 * 60, 0)),
                    labels());
            assertNextNode(
                    nodes,
                    1L,
                    properties("name", "Johan", "duration", DurationValue.duration(-12, 0, 4 * 3600 + 20 * 60, 0)),
                    labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldHaveNodesBelongToGroupSpecifiedInHeader() throws Exception {
        // GIVEN
        Group group = groups.getOrCreate("MyGroup");
        String idHeader = ":ID(%s)".formatted(group.name());
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(idHeader),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name")),
                List.of(new Object[] {123, "one"}, new Object[] {456, "two"}));
        Input input = new ParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, ';', groups, MONITOR);
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, group, 123L, properties("name", "one"), labels());
            assertNextNode(nodes, group, 456L, properties("name", "two"), labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldHaveRelationshipsSpecifyStartEndNodeIdGroupsInHeader() throws Exception {
        // GIVEN
        var startGroupName = "StartGroup";
        var endGroupName = "EndGroup";
        Path relationshipFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32)
                                .named(":START_ID(%s)".formatted(startGroupName)),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":TYPE"),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32)
                                .named(":END_ID(%s)".formatted(endGroupName))),
                List.of(new Object[] {123, "TYPE", 234}, new Object[] {345, "TYPE", 456}));
        Path nodeFile1 = createParquetFile(
                List.of(Types.required(PrimitiveType.PrimitiveTypeName.INT32)
                        .named(":ID(%s)".formatted(startGroupName))),
                List.of());
        Path nodeFile2 = createParquetFile(
                List.of(Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID(%s)".formatted(endGroupName))),
                List.of());
        Input input = new ParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile1, nodeFile2})),
                Map.of("", List.<Path[]>of(new Path[] {relationshipFile})),
                INTEGER,
                ';',
                groups,
                MONITOR);
        // WHEN/THEN
        try (InputIterator relationships = input.relationships(EMPTY).iterator()) {
            assertRelationship(relationships, startGroupName, 123L, endGroupName, 234L, "TYPE", properties());
            assertRelationship(relationships, startGroupName, 345L, endGroupName, 456L, "TYPE", properties());
            assertFalse(readNext(relationships));
        }
    }

    @Test
    void shouldDoWithoutRelationshipTypeHeaderIfDefaultSupplied() throws Exception {
        // GIVEN relationship data w/o :TYPE column
        String defaultType = "HERE";
        Path relationshipFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":START_ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":END_ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name")),
                List.of(new Object[] {0, 1, "First"}, new Object[] {2, 3, "Second"}));
        Path nodeFile = createParquetFile(
                List.of(Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID")), List.of());
        Input input = new ParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})),
                Map.of(defaultType, List.<Path[]>of(new Path[] {relationshipFile})),
                INTEGER,
                ';',
                groups,
                MONITOR);
        // WHEN
        try (InputIterator relationships = input.relationships(EMPTY).iterator()) {
            // THEN
            assertNextRelationship(relationships, 0L, 1L, defaultType, properties("name", "First"));
            assertNextRelationship(relationships, 2L, 3L, defaultType, properties("name", "Second"));
            assertFalse(readNext(relationships));
        }
    }

    @Test
    void shouldIgnoreNodeEntriesMarkedIgnoreUsingHeader() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name:IGNORE"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("other:int"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":LABEL")),
                List.of(
                        new Object[] {1, "Mattias", "10", "Person"},
                        new Object[] {2, "Johan", "111", "Person"},
                        new Object[] {3, "Emil", "12", "Person"}));
        Input input = new ParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, ';', groups, MONITOR);

        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 1L, properties("other", 10), labels("Person"));
            assertNextNode(nodes, 2L, properties("other", 111), labels("Person"));
            assertNextNode(nodes, 3L, properties("other", 12), labels("Person"));
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldIgnoreRelationshipEntriesMarkedIgnoreUsingHeader() throws Exception {
        // GIVEN
        Path relationshipFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":START_ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":TYPE"),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":END_ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("prop:IGNORE"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("other:int")),
                List.of(
                        new Object[] {1, "KNOWS", 2, "Mattias", "10"},
                        new Object[] {2, "KNOWS", 3, "Johan", "111"},
                        new Object[] {3, "KNOWS", 4, "Emil", "12"}));
        Path nodeFile = createParquetFile(
                List.of(Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID")), List.of());
        Input input = new ParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})),
                Map.of("", List.<Path[]>of(new Path[] {relationshipFile})),
                INTEGER,
                ';',
                new Groups(),
                MONITOR);

        // WHEN
        try (InputIterator relationships = input.relationships(EMPTY).iterator()) {
            assertNextRelationship(relationships, 1L, 2L, "KNOWS", properties("other", 10));
            assertNextRelationship(relationships, 2L, 3L, "KNOWS", properties("other", 111));
            assertNextRelationship(relationships, 3L, 4L, "KNOWS", properties("other", 12));
            assertFalse(readNext(relationships));
        }
    }

    @Test
    void shouldNotIncludeEmptyArraysInEntities() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("sprop:String[]"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("lprop:long[]")),
                List.of(new Object[] {1, "", ""}, new Object[] {2, "a;b", "10;20"}));
        Input input = new ParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, ';', groups, MONITOR);

        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 1L, emptyMap(), labels());
            assertNextNode(
                    nodes, 2L, properties("sprop", new String[] {"a", "b"}, "lprop", new long[] {10, 20}), labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldNotIncludeNullArraysInEntities() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("sprop:String[]"),
                        Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("lprop:long[]")),
                List.of(new Object[] {1, null, null}, new Object[] {2, "a;b", "10;20"}));
        Input input = new ParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, ';', groups, MONITOR);

        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 1L, emptyMap(), labels());
            assertNextNode(
                    nodes, 2L, properties("sprop", new String[] {"a", "b"}, "lprop", new long[] {10, 20}), labels());
            assertFalse(readNext(nodes));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {":SOMETHING", "abcde#rtg:123", "", ":START_ID", ":END_ID", ":TYPE"})
    void shouldFailOnUnparsableNodeColumn(String unparsableColumnNames) throws Exception {
        // given
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(unparsableColumnNames)),
                List.<Object[]>of(new Object[] {1, "test"}));
        try {
            // when
            new ParquetInput(
                    Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})),
                    Map.of(),
                    INTEGER,
                    ';',
                    groups,
                    MONITOR);
            fail("Should not parse");
        } catch (InputException e) {
            // then
            // OK
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {":SOMETHING", "abcde#rtg:123", ":ID", ":LABEL"})
    void shouldFailOnUnparsableRelationshipHeader(String unparsableColumnName) throws Exception {
        // given
        Path relationshipFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":START_ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":END_ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":TYPE"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(unparsableColumnName)),
                List.<Object[]>of(new Object[] {1, 2, "TYPE", "test"}));
        try {
            // when
            new ParquetInput(
                    Map.of(Set.of(""), List.of()),
                    Map.of("", List.<Path[]>of(new Path[] {relationshipFile})),
                    INTEGER,
                    ';',
                    groups,
                    MONITOR);
            fail("Should not parse");
        } catch (InputException e) {
            // then
            // OK
        }
    }

    @Test
    void shouldFailOnUndefinedGroupInRelationshipHeader() throws Exception {
        // given
        Path relationshipFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":START_ID(left)"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":TYPE"),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":END_ID(rite)")),
                List.of(new Object[] {123, "TYPE", 234}, new Object[] {345, "TYPE", 456}));
        Path nodeFile1 = createParquetFile(
                List.of(Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID(left)")), List.of());
        Path nodeFile2 = createParquetFile(
                List.of(Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID(right)")), List.of());
        try {
            // when
            new ParquetInput(
                    Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile1, nodeFile2})),
                    Map.of("", List.<Path[]>of(new Path[] {relationshipFile})),
                    INTEGER,
                    ';',
                    groups,
                    MONITOR);
            fail("Should not validate");
        } catch (InputException e) {
            // then
            // OK
        }
    }

    @Test
    void shouldFailOnGlobalGroupInRelationshipHeaderIfNoGlobalGroupInNodeHeader() throws Exception {
        // given
        Path relationshipFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":START_ID(left)"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":TYPE"),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":END_ID")),
                List.of(new Object[] {123, "TYPE", 234}, new Object[] {345, "TYPE", 456}));
        Path nodeFile1 = createParquetFile(
                List.of(Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID(left)")), List.of());
        Path nodeFile2 = createParquetFile(
                List.of(Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID(right)")), List.of());
        try {
            // when
            new ParquetInput(
                    Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile1, nodeFile2})),
                    Map.of("", List.<Path[]>of(new Path[] {relationshipFile})),
                    INTEGER,
                    ';',
                    new Groups(),
                    MONITOR); // new Groups() instead of field groups important here to not have the global id space
            fail("Should not validate");
        } catch (InputException e) {
            // then
            // OK
        }
    }

    @Test
    void shouldNormalizeTypes() throws Exception {
        // given
        Path relationshipFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":START_ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":END_ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("byteProp:byte"),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT64).named("longProp:long")),
                List.<Object[]>of(new Object[] {123, 234, 8, 123L}));
        Path nodeFile1 = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("shortProp:short"),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("intProp:int")),
                List.<Object[]>of(new Object[] {1, 234, 1024}));
        Path nodeFile2 = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.FLOAT).named("floatProp:float"),
                        Types.required(PrimitiveType.PrimitiveTypeName.DOUBLE).named("doubleProp")),
                List.<Object[]>of(new Object[] {2, 43f, 37d}));
        ParquetMonitor monitor = mock(ParquetMonitor.class);

        // when
        new ParquetInput(
                Map.of(Set.of("someLabel"), List.<Path[]>of(new Path[] {nodeFile1, nodeFile2})),
                Map.of("someType", List.<Path[]>of(new Path[] {relationshipFile})),
                INTEGER,
                ';',
                groups,
                monitor);

        // then
        verify(monitor, times(1)).typeNormalized("test1.parquet", "intProp", "INT", "LONG");
        verify(monitor, times(1)).typeNormalized("test1.parquet", "shortProp", "SHORT", "LONG");
        verify(monitor, times(1)).typeNormalized("test2.parquet", "floatProp", "FLOAT", "DOUBLE");
        verify(monitor, times(1)).typeNormalized("test0.parquet", "byteProp", "BYTE", "LONG");
        verifyNoMoreInteractions(monitor);
    }

    @Test
    void shouldReportNoNodeLabels() throws Exception {
        // given
        Path nodeFile = createParquetFile(
                List.of(Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID")),
                List.<Object[]>of(new Object[] {1}));
        ParquetMonitor monitor = mock(ParquetMonitor.class);

        // when
        new ParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, ';', groups, monitor);
        // then
        verify(monitor).noNodeLabelsSpecified("test0.parquet");
    }

    @Test
    void shouldNotReportNoNodeLabelsIfDecorated() throws Exception {
        // given
        Path nodeFile = createParquetFile(
                List.of(Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID")),
                List.<Object[]>of(new Object[] {1}));
        ParquetMonitor monitor = mock(ParquetMonitor.class);

        // when
        new ParquetInput(
                Map.of(Set.of("test"), List.<Path[]>of(new Path[] {nodeFile})),
                Map.of(),
                INTEGER,
                ';',
                groups,
                monitor);

        // then
        verify(monitor, never()).noNodeLabelsSpecified("test0.parquet");
    }

    @Test
    void shouldReportNoRelationshipType() throws Exception {
        // given
        Path relationshipFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":START_ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":END_ID")),
                List.<Object[]>of(new Object[] {1, 2}));
        ParquetMonitor monitor = mock(ParquetMonitor.class);

        // when
        new ParquetInput(
                Map.of(), Map.of("", List.<Path[]>of(new Path[] {relationshipFile})), INTEGER, ';', groups, monitor);

        // then
        verify(monitor).noRelationshipTypeSpecified("test0.parquet");
    }

    @Test
    void shouldNotReportNoRelationshipTypeIfDecorated() throws Exception {
        // given
        Path relationshipFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":START_ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":END_ID")),
                List.<Object[]>of(new Object[] {1, 2}));
        ParquetMonitor monitor = mock(ParquetMonitor.class);

        // when
        new ParquetInput(
                Map.of(),
                Map.of("someType", List.<Path[]>of(new Path[] {relationshipFile})),
                INTEGER,
                ';',
                groups,
                monitor);
        // then
        verify(monitor, never()).noRelationshipTypeSpecified("test0.parquet");
    }

    @Test
    void shouldReportDuplicateNodeHeader() throws Exception {
        // given
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name:string"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name")),
                List.of());
        try {
            // when
            new ParquetInput(
                    Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})),
                    Map.of(),
                    INTEGER,
                    ';',
                    groups,
                    new ParquetMonitor(System.out));
            fail("Should have failed");
        } catch (DuplicatedColumnException e) {
            // THEN
            assertThat(e).hasMessageContaining("test0.parquet");
        }
    }

    @Test
    void shouldReportDuplicateRelationshipHeader() throws Exception {
        // given
        Path relationshipFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":START_ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":TYPE"),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":END_ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":TYPE"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name")),
                List.of());
        try {
            // when
            new ParquetInput(
                    Map.of(),
                    Map.of("", List.<Path[]>of(new Path[] {relationshipFile})),
                    INTEGER,
                    ';',
                    groups,
                    new ParquetMonitor(System.out));
            fail("Should have failed");
        } catch (DuplicatedColumnException e) {
            // THEN
            assertThat(e).hasMessageContaining("test0.parquet");
        }
    }

    @Test
    void shouldThrowOnReferencedNodeSchemaWithoutExplicitLabelOptionData() throws Exception {
        // given
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("my:ID(Person)"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name:string"),
                        Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":LABEL")),
                List.of());
        try (var input = new ParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})),
                Map.of(),
                STRING,
                ';',
                groups,
                new ParquetMonitor(System.out))) {
            // when
            var tokenHolders = new TokenHolders(
                    tokenHolder(Map.of("myId", 4)), tokenHolder(Map.of("Person", 2)), tokenHolder(Map.of()));

            // then
            assertThatThrownBy(() -> input.referencedNodeSchema(tokenHolders))
                    .hasMessageContaining("No label was specified");
        }
    }

    @Test
    void shouldHandleMultipleEqualReferencedSchemaForSameGroup() throws Exception {
        // given
        Path nodeFile1 = createParquetFile(
                List.of(Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("myId:ID(MyGroup){label:Person}")),
                List.of());
        Path nodeFile2 = createParquetFile(
                List.of(Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("myId:ID(MyGroup){label:Person}")),
                List.of());
        try (var input = new ParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile1, nodeFile2})),
                Map.of(),
                STRING,
                ';',
                groups,
                new ParquetMonitor(System.out))) {
            // when
            var tokenHolders = new TokenHolders(
                    tokenHolder(Map.of("myId", 4)), tokenHolder(Map.of("Person", 2)), tokenHolder(Map.of()));

            // then
            var referencedNodeSchema = input.referencedNodeSchema(tokenHolders);
            assertThat(referencedNodeSchema.get("MyGroup"))
                    .isEqualTo(SchemaDescriptors.forLabel(
                            tokenHolders.labelTokens().getIdByName("Person"),
                            tokenHolders.propertyKeyTokens().getIdByName("myId")));
        }
    }

    @Test
    void shouldFailMultipleNonEqualReferencedSchemaForSameGroup() throws Exception {
        // given
        Path nodeFile1 = createParquetFile(
                List.of(Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("myId:ID(MyGroup){label:Person}")),
                List.of());
        Path nodeFile2 = createParquetFile(
                List.of(Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("myId:ID(MyGroup){label:Company}")),
                List.of());
        try (var input = new ParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile1, nodeFile2})),
                Map.of(),
                STRING,
                ';',
                groups,
                new ParquetMonitor(System.out))) {
            // when
            var tokenHolders = new TokenHolders(
                    tokenHolder(Map.of("myId", 4)),
                    tokenHolder(Map.of("Person", 2, "Company", 3)),
                    tokenHolder(Map.of()));

            // then
            assertThatThrownBy(() -> input.referencedNodeSchema(tokenHolders))
                    .hasMessageContaining("Multiple different indexes for group");
        }
    }

    @Test
    void shouldParseReferencedNodeSchemaWithExplicitLabelOptionData() throws Exception {
        // given
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("myId:ID(My Group){label:Person}"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name:string"),
                        Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":LABEL")),
                List.of());
        try (var input = new ParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})),
                Map.of(),
                STRING,
                ';',
                groups,
                new ParquetMonitor(System.out))) {
            // when
            var tokenHolders = new TokenHolders(
                    tokenHolder(Map.of("myId", 4)), tokenHolder(Map.of("Person", 2)), tokenHolder(Map.of()));
            var schema = input.referencedNodeSchema(tokenHolders);

            // then
            Assertions.assertThat(schema).isEqualTo(Map.of("My Group", SchemaDescriptors.forLabel(2, 4)));
        }
    }

    @Test
    void shouldStoreIdAsPropertyInSpecificValueType() throws Exception {
        // given nodes w/ IDs as ints
        // when using string id-type in the input
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("id:ID{id-type:int}"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("prop")),
                List.<Object[]>of(new Object[] {123, "val"}));
        try (var input = new ParquetInput(
                        Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})),
                        Map.of(),
                        STRING,
                        ';',
                        groups,
                        new ParquetMonitor(System.out));
                var nodes = input.nodes(EMPTY).iterator()) {
            // then
            assertNextNode(nodes, 123, properties("id", 123, "prop", "val"), labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldHandleMultipleNodeIdColumns() throws Exception {
        // given
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("id1:ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("id2:ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":LABEL")),
                List.of(new Object[] {"ABC", "123", "First", "Person"}, new Object[] {"ABC", "456", "Second", "Person"
                }));
        try (var input = new ParquetInput(
                        Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})),
                        Map.of(),
                        STRING,
                        ';',
                        groups,
                        new ParquetMonitor(System.out));
                var nodes = input.nodes(Collector.STRICT).iterator()) {
            assertNextNode(nodes, "ABC123", properties("id1", "ABC", "id2", "123", "name", "First"), Set.of("Person"));
            assertNextNode(nodes, "ABC456", properties("id1", "ABC", "id2", "456", "name", "Second"), Set.of("Person"));
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldFailOnStoringMultipleCompositeIdColumnsInSameProperty() throws Exception {
        // given
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("id:ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("id:ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":LABEL")),
                List.of(new Object[] {"ABC", "123", "First", "Person"}, new Object[] {"ABC", "456", "Second", "Person"
                }));
        // when/then
        assertThatThrownBy(() -> new ParquetInput(
                        Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})),
                        Map.of(),
                        STRING,
                        ';',
                        groups,
                        new ParquetMonitor(System.out)))
                .isInstanceOf(InputException.class)
                .hasMessageContaining("Cannot store composite IDs");
    }

    @Test
    void shouldFailOnCompositeIdColumnsForDifferentGroups() throws Exception {
        // given
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":ID(group1)"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":ID(group2)"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":LABEL")),
                List.of(new Object[] {"ABC", "123", "First", "Person"}, new Object[] {"ABC", "456", "Second", "Person"
                }));
        // when/then
        assertThatThrownBy(() -> new ParquetInput(
                        Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})),
                        Map.of(),
                        INTEGER,
                        ';',
                        groups,
                        new ParquetMonitor(System.out)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("referring to different groups");
    }

    @Test
    void shouldFailOnNonParquetFile() throws Exception {
        Path nodeFile = createNonParquetFile();
        assertThatThrownBy(() -> new ParquetInput(
                        Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})),
                        Map.of(),
                        INTEGER,
                        ';',
                        groups,
                        new ParquetMonitor(System.out)))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Could not read parquet file %s".formatted(nodeFile));
    }

    private Path createNonParquetFile() throws Exception {
        Path path = directory.file("test-non.parquet");
        try (var writer = new FileWriter(path.toFile())) {
            writer.write("some data for sure not parquet");
        }
        return path;
    }

    private Path createParquetFile(List<org.apache.parquet.schema.Type> types, List<Object[]> data) throws Exception {
        Path path = directory.file("test%d.parquet".formatted(fileCounter++));
        try (var writer =
                ParquetWriter.writeFile(new MessageType("something", types), path.toFile(), (record, valueWriter) -> {
                    var recordData = (Object[]) record;
                    for (int i = 0; i < types.size(); i++) {
                        org.apache.parquet.schema.Type type = types.get(i);
                        Object value = recordData[i];
                        if (value != null) {
                            valueWriter.write(type.getName(), value);
                        }
                    }
                })) {
            for (Object[] datum : data) {
                writer.write(datum);
            }
        }

        return path;
    }

    private TokenHolder tokenHolder(Map<String, Integer> tokens) {
        var tokenHolder = new CreatingTokenHolder(ReadOnlyTokenCreator.READ_ONLY, "type");
        tokenHolder.setInitialTokens(tokens.entrySet().stream()
                .map(e -> new NamedToken(e.getKey(), e.getValue()))
                .toList());
        return tokenHolder;
    }

    private static void assertEstimatesEquals(Input.Estimates a, Input.Estimates b, double errorMargin) {
        assertEquals(a.numberOfNodes(), b.numberOfNodes(), a.numberOfNodes() * errorMargin);
        assertEquals(a.numberOfNodeLabels(), b.numberOfNodeLabels(), a.numberOfNodeLabels() * errorMargin);
        assertEquals(a.numberOfNodeProperties(), b.numberOfNodeProperties(), a.numberOfNodeProperties() * errorMargin);
        assertEquals(a.numberOfRelationships(), b.numberOfRelationships(), a.numberOfRelationships() * errorMargin);
        assertEquals(
                a.numberOfRelationshipProperties(),
                b.numberOfRelationshipProperties(),
                a.numberOfRelationshipProperties() * errorMargin);
        assertEquals(a.sizeOfNodeProperties(), b.sizeOfNodeProperties(), a.sizeOfNodeProperties() * errorMargin);
        assertEquals(
                a.sizeOfRelationshipProperties(),
                b.sizeOfRelationshipProperties(),
                a.sizeOfRelationshipProperties() * errorMargin);
    }

    private static Input.Estimates calculateEstimatesOnSingleFileNodeData(IdType idType, Path nodeDataFile)
            throws IOException {
        Input input = new ParquetInput(Map.of(), Map.of(), INTEGER, ';', new Groups(), MONITOR);
        // We don't care about correct value size calculation really, as long as it's consistent
        return input.validateAndEstimate((values, tracer, memTracker) ->
                Stream.of(values).mapToInt(v -> v.toString().length()).sum());
    }

    private Path createNodeInputDataFile(long roughSize) throws FileNotFoundException {
        Path file = directory.file("data-file");
        MutableLong bytesWritten = new MutableLong();
        BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(file.toFile())) {
            @Override
            public synchronized void write(byte[] b, int off, int len) throws IOException {
                super.write(b, off, len);
                bytesWritten.add(len);
            }

            @Override
            public synchronized void write(int b) throws IOException {
                super.write(b);
                bytesWritten.add(1);
            }

            @Override
            public void write(byte[] b) throws IOException {
                super.write(b);
                bytesWritten.add(b.length);
            }
        };
        try (PrintWriter writer = new PrintWriter(out)) {
            writer.println(":ID,name:string,prop:int");
            while (bytesWritten.longValue() < roughSize) {
                writer.println(format(
                        "%s,%s,%d",
                        random.nextAlphaNumericString(6, 6), random.nextAlphaNumericString(5, 20), random.nextInt()));
            }
        }
        return file;
    }

    private void assertNextRelationship(
            InputIterator relationship, Object startNode, Object endNode, String type, Map<String, Object> properties)
            throws IOException {
        assertRelationship(relationship, globalGroup, startNode, globalGroup, endNode, type, properties);
    }

    private void assertRelationship(
            InputIterator data,
            Group startNodeGroup,
            Object startNode,
            Group endNodeGroup,
            Object endNode,
            String type,
            Map<String, Object> properties)
            throws IOException {
        assertTrue(readNext(data));
        assertEquals(startNodeGroup, visitor.startIdGroup);
        assertEquals(startNode, visitor.startId());
        assertEquals(endNodeGroup, visitor.endIdGroup);
        assertEquals(endNode, visitor.endId());
        assertEquals(type, visitor.stringType);
        assertPropertiesEquals(properties, visitor.propertiesAsMap());
    }

    private void assertRelationship(
            InputIterator data,
            String startNodeGroupName,
            Object startNode,
            String endNodeGroupName,
            Object endNode,
            String type,
            Map<String, Object> properties)
            throws IOException {
        assertTrue(readNext(data));
        assertEquals(startNodeGroupName, visitor.startIdGroup.name());
        assertEquals(startNode, visitor.startId());
        assertEquals(endNodeGroupName, visitor.endIdGroup.name());
        assertEquals(endNode, visitor.endId());
        assertEquals(type, visitor.stringType);
        assertPropertiesEquals(properties, visitor.propertiesAsMap());
    }

    private void assertNextNode(InputIterator data, Object id, Map<String, Object> properties, Set<String> labels)
            throws IOException {
        assertNextNode(data, globalGroup, id, properties, labels);
    }

    private void assertNextNode(
            InputIterator data, Group group, Object id, Map<String, Object> properties, Set<String> labels)
            throws IOException {
        assertTrue(readNext(data));
        assertEquals(group, visitor.idGroup);
        assertEquals(id, visitor.id());
        assertEquals(labels, asSet(visitor.labels()));
        assertPropertiesEquals(properties, visitor.propertiesAsMap());
    }

    private void assertPropertiesEquals(Map<String, Object> expected, Map<String, Object> actual) {
        // Do this more complicated assert to handle primitive array equality
        assertEquals(primitiveArraysAsLists(expected), primitiveArraysAsLists(actual));
    }

    private Map<String, Object> primitiveArraysAsLists(Map<String, Object> map) {
        Map<String, Object> result = new HashMap<>();
        for (var entry : map.entrySet()) {
            var value = entry.getValue();
            var cls = value.getClass();
            if (cls.isArray()) {
                List<Object> listValue = new ArrayList<>();
                var length = Array.getLength(value);
                for (var i = 0; i < length; i++) {
                    listValue.add(Array.get(value, i));
                }
                value = listValue;
            }
            result.put(entry.getKey(), value);
        }
        return result;
    }

    private boolean readNext(InputIterator data) throws IOException {
        if (referenceData != data) {
            chunk = null;
            referenceData = data;
        }

        if (chunk == null) {
            chunk = data.newChunk();
            if (!data.next(chunk)) {
                return false;
            }
        }

        if (chunk.next(visitor)) {
            return true;
        }
        if (!data.next(chunk)) {
            return false;
        }
        return chunk.next(visitor);
    }

    private static Map<String, Object> properties(Object... keysAndValues) {
        return MapUtil.map(keysAndValues);
    }

    private static Set<String> labels(String... labels) {
        return asSet(labels);
    }
}
