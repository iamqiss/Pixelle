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
package org.neo4j.batchimport.api.input;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.neo4j.batchimport.api.BatchImporter;
import org.neo4j.batchimport.api.InputIterable;
import org.neo4j.batchimport.api.InputIterator;
import org.neo4j.internal.schema.SchemaCommand;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaTokens;
import org.neo4j.token.TokenHolders;

/**
 * Unifies all data input given to a {@link BatchImporter} to allow for more coherent implementations.
 */
public interface Input extends AutoCloseable {
    /**
     * @param numberOfNodes estimated number of nodes for the entire input.
     * @param numberOfRelationships estimated number of relationships for the entire input.
     * @param numberOfNodeProperties estimated number of node properties.
     * @param numberOfRelationshipProperties estimated number of relationship properties.
     * @param sizeOfNodeProperties estimated size that the estimated number of node properties will require on disk.
     * This is a separate estimate since it depends on the type and size of the actual properties.
     * @param sizeOfRelationshipProperties estimated size that the estimated number of relationship properties will require on disk.
     * This is a separate estimate since it depends on the type and size of the actual properties.
     * @param numberOfNodeLabels estimated number of node labels. Examples:
     * <ul>
     * <li>2 nodes, 1 label each ==> 2</li>
     * <li>1 node, 2 labels each ==> 2</li>
     * <li>2 nodes, 2 labels each ==> 4</li>
     * </ul>
     */
    record Estimates(
            long numberOfNodes,
            long numberOfRelationships,
            long numberOfNodeProperties,
            long numberOfRelationshipProperties,
            long sizeOfNodeProperties,
            long sizeOfRelationshipProperties,
            long numberOfNodeLabels) {}

    /**
     * Provides all node data for an import.
     *
     * @param badCollector for collecting bad entries.
     * @return an {@link InputIterator} which will provide all node data for the whole import.
     */
    InputIterable nodes(Collector badCollector);

    /**
     * Provides all relationship data for an import.
     *
     * @param badCollector for collecting bad entries.
     * @return an {@link InputIterator} which will provide all relationship data for the whole import.
     */
    InputIterable relationships(Collector badCollector);

    /**
     * @return {@link IdType} which matches the type of ids this {@link Input} generates.
     * Will get populated by node import and later queried by relationship import
     * to resolve potentially temporary input node ids to actual node ids in the database.
     */
    IdType idType();

    /**
     * @return accessor for id groups that this input has.
     */
    ReadableGroups groups();

    /**
     * Validates the {@link Input} and estimates the expected size of the graph using a small, initial section of the
     * data and extrapolating using the full size of the data.
     * <br>
     * Examples of types of validation could include:
     * <ul>
     *     <li>checking the configuration provided is valid and consistent</li>
     *     <li>that no duplicated resource are present in the node/relationship inputs</li>
     *     <li>that any header information is valid and consistent</li>
     * </ul>
     * @param valueSizeCalculator for calculating property sizes on disk.
     * @return {@link Estimates} for this input w/o reading through it entirely.
     * @throws IOException on I/O error.
     */
    Estimates validateAndEstimate(PropertySizeCalculator valueSizeCalculator) throws IOException;

    /**
     * @return a {@link Map} where key is group name and value which {@link SchemaDescriptor index} it refers to.
     * @param tokenHolders available tokens.
     */
    default Map<String, SchemaDescriptor> referencedNodeSchema(TokenHolders tokenHolders) {
        throw new UnsupportedOperationException();
    }

    /**
     * @return the schema commands to be applied after the data has been imported.
     */
    default List<SchemaCommand> schemaCommands() {
        return List.of();
    }

    /**
     * @return all the tokens required by the schema entities specified by this input
     */
    default SchemaTokens schemaTokens() {
        return SchemaTokens.collect(schemaCommands());
    }

    @Override
    default void close() {}

    static Input input(
            InputIterable nodes,
            InputIterable relationships,
            IdType idType,
            Estimates estimates,
            ReadableGroups groups) {
        return new Input() {
            @Override
            public InputIterable relationships(Collector badCollector) {
                return relationships;
            }

            @Override
            public InputIterable nodes(Collector badCollector) {
                return nodes;
            }

            @Override
            public IdType idType() {
                return idType;
            }

            @Override
            public ReadableGroups groups() {
                return groups;
            }

            @Override
            public Estimates validateAndEstimate(PropertySizeCalculator valueSizeCalculator) {
                return estimates;
            }
        };
    }

    static Estimates knownEstimates(
            long numberOfNodes,
            long numberOfRelationships,
            long numberOfNodeProperties,
            long numberOfRelationshipProperties,
            long sizeOfNodeProperties,
            long sizeOfRelationshipProperties,
            long numberOfNodeLabels) {
        return new Estimates(
                numberOfNodes,
                numberOfRelationships,
                numberOfNodeProperties,
                numberOfRelationshipProperties,
                sizeOfNodeProperties,
                sizeOfRelationshipProperties,
                numberOfNodeLabels);
    }

    class Delegate implements Input {

        protected final Input delegate;

        public Delegate(Input delegate) {
            this.delegate = delegate;
        }

        @Override
        public InputIterable relationships(Collector badCollector) {
            return delegate.relationships(badCollector);
        }

        @Override
        public InputIterable nodes(Collector badCollector) {
            return delegate.nodes(badCollector);
        }

        @Override
        public IdType idType() {
            return delegate.idType();
        }

        @Override
        public ReadableGroups groups() {
            return delegate.groups();
        }

        @Override
        public Estimates validateAndEstimate(PropertySizeCalculator valueSizeCalculator) throws IOException {
            return delegate.validateAndEstimate(valueSizeCalculator);
        }

        @Override
        public Map<String, SchemaDescriptor> referencedNodeSchema(TokenHolders tokenHolders) {
            return delegate.referencedNodeSchema(tokenHolders);
        }

        @Override
        public List<SchemaCommand> schemaCommands() {
            return delegate.schemaCommands();
        }

        @Override
        public void close() {
            delegate.close();
        }
    }
}
