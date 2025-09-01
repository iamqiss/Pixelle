/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.density.transport.grpc.proto.request.search.query;

import org.density.core.xcontent.XContentParser;
import org.density.index.query.AbstractQueryBuilder;
import org.density.index.query.QueryBuilder;
import org.density.protobufs.QueryContainer;

/**
 * Utility class for converting Protocol Buffer query representations to Density QueryBuilder objects.
 * This class provides methods to parse different types of query containers and transform them
 * into their corresponding Density QueryBuilder implementations for search operations.
 */
public class AbstractQueryBuilderProtoUtils {

    private final QueryBuilderProtoConverterRegistry registry;

    /**
     * Creates a new instance with the specified registry.
     *
     * @param registry The registry to use for query conversion
     * @throws IllegalArgumentException if registry is null
     */
    public AbstractQueryBuilderProtoUtils(QueryBuilderProtoConverterRegistry registry) {
        if (registry == null) {
            throw new IllegalArgumentException("Registry cannot be null");
        }
        this.registry = registry;
    }

    /**
     * Parse a query from its Protocol Buffer representation.
     * Similar to {@link AbstractQueryBuilder#parseInnerQueryBuilder(XContentParser)}, this method
     * determines the query type from the Protocol Buffer container and delegates to the appropriate
     * specialized parser.
     *
     * @param queryContainer The Protocol Buffer query container that holds various query type options
     * @return A QueryBuilder instance configured according to the input query parameters
     * @throws UnsupportedOperationException if the query type is not supported
     */
    public QueryBuilder parseInnerQueryBuilderProto(QueryContainer queryContainer) throws UnsupportedOperationException {
        // Validate input
        if (queryContainer == null) {
            throw new IllegalArgumentException("Query container cannot be null");
        }

        return registry.fromProto(queryContainer);
    }
}
