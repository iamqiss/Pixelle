/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.density.transport.grpc.proto.request.search.query;

import org.density.core.xcontent.XContentParser;
import org.density.index.query.MatchNoneQueryBuilder;
import org.density.protobufs.MatchNoneQuery;

/**
 * Utility class for converting MatchNoneQuery Protocol Buffers to Density objects.
 * This class provides methods to transform Protocol Buffer representations of match_none queries
 * into their corresponding Density MatchNoneQueryBuilder implementations for search operations.
 */
public class MatchNoneQueryBuilderProtoUtils {

    private MatchNoneQueryBuilderProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a Protocol Buffer MatchNoneQuery to an Density MatchNoneQueryBuilder.
     * Similar to {@link MatchNoneQueryBuilder#fromXContent(XContentParser)}, this method
     * parses the Protocol Buffer representation and creates a properly configured
     * MatchNoneQueryBuilder with the appropriate boost and name settings.
     *
     * @param matchNoneQueryProto The Protocol Buffer MatchNoneQuery to convert
     * @return A configured MatchNoneQueryBuilder instance
     */
    protected static MatchNoneQueryBuilder fromProto(MatchNoneQuery matchNoneQueryProto) {
        MatchNoneQueryBuilder matchNoneQueryBuilder = new MatchNoneQueryBuilder();

        if (matchNoneQueryProto.hasBoost()) {
            matchNoneQueryBuilder.boost(matchNoneQueryProto.getBoost());
        }

        if (matchNoneQueryProto.hasUnderscoreName()) {
            matchNoneQueryBuilder.queryName(matchNoneQueryProto.getUnderscoreName());
        }

        return matchNoneQueryBuilder;
    }
}
