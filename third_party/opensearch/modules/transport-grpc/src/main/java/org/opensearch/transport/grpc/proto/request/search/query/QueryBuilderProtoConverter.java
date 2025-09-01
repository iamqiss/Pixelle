/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.density.transport.grpc.proto.request.search.query;

import org.density.index.query.QueryBuilder;
import org.density.protobufs.QueryContainer;

/**
 * Interface for converting protobuf query messages to Density QueryBuilder objects.
 * External plugins can implement this interface to provide their own query types.
 */
public interface QueryBuilderProtoConverter {

    /**
     * Returns the QueryContainerCase this converter handles.
     *
     * @return The QueryContainerCase
     */
    QueryContainer.QueryContainerCase getHandledQueryCase();

    /**
     * Converts a protobuf query container to an Density QueryBuilder.
     *
     * @param queryContainer The protobuf query container
     * @return The corresponding Density QueryBuilder
     * @throws IllegalArgumentException if the query cannot be converted
     */
    QueryBuilder fromProto(QueryContainer queryContainer);
}
