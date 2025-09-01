/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.density.transport.grpc.proto.request.search.query;

import org.density.core.xcontent.XContentParser;
import org.density.index.query.MatchAllQueryBuilder;
import org.density.protobufs.MatchAllQuery;

/**
 * Utility class for converting MatchAllQuery Protocol Buffers to Density query objects.
 */
public class MatchAllQueryBuilderProtoUtils {

    private MatchAllQueryBuilderProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a Protocol Buffer MatchAllQuery to an Density MatchAllQueryBuilder.
     * Similar to {@link MatchAllQueryBuilder#fromXContent(XContentParser)}, this method
     * parses the Protocol Buffer representation and creates a properly configured
     * MatchAllQueryBuilder with the appropriate boost and name settings.
     *
     * @param matchAllQueryProto The Protocol Buffer MatchAllQuery to convert
     * @return A configured MatchAllQueryBuilder instance
     */
    protected static MatchAllQueryBuilder fromProto(MatchAllQuery matchAllQueryProto) {
        MatchAllQueryBuilder matchAllQueryBuilder = new MatchAllQueryBuilder();

        if (matchAllQueryProto.hasBoost()) {
            matchAllQueryBuilder.boost(matchAllQueryProto.getBoost());
        }

        if (matchAllQueryProto.hasUnderscoreName()) {
            matchAllQueryBuilder.queryName(matchAllQueryProto.getUnderscoreName());
        }

        return matchAllQueryBuilder;
    }
}
