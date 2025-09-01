/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.transport.grpc.proto.request.search;

import org.density.index.query.QueryBuilder;
import org.density.index.query.QueryBuilders;
import org.density.index.query.QueryStringQueryBuilder;
import org.density.protobufs.SearchRequest;
import org.density.rest.RestRequest;
import org.density.rest.action.RestActions;

/**
 * Utility class for converting REST-like actions between Density and Protocol Buffers formats.
 * This class provides methods to transform URL parameters from Protocol Buffer requests into
 * query builders and other Density constructs.
 */
public class ProtoActionsProtoUtils {

    private ProtoActionsProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Similar to {@link RestActions#urlParamsToQueryBuilder(RestRequest)}
     *
     * @param request
     * @return
     */
    protected static QueryBuilder urlParamsToQueryBuilder(SearchRequest request) {
        if (!request.hasQ()) {
            return null;
        }

        QueryStringQueryBuilder queryBuilder = QueryBuilders.queryStringQuery(request.getQ());
        queryBuilder.defaultField(request.hasDf() ? request.getDf() : null);
        queryBuilder.analyzer(request.hasAnalyzer() ? request.getAnalyzer() : null);
        queryBuilder.analyzeWildcard(request.hasAnalyzeWildcard() ? request.getAnalyzeWildcard() : false);
        queryBuilder.lenient(request.hasLenient() ? request.getLenient() : null);
        if (request.hasDefaultOperator()) {
            queryBuilder.defaultOperator(OperatorProtoUtils.fromEnum(request.getDefaultOperator()));
        }
        return queryBuilder;
    }
}
