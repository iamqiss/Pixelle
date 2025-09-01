/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.density.transport.grpc.proto.response.search;

import org.density.action.search.SearchResponse;
import org.density.action.search.SearchResponseSections;
import org.density.core.xcontent.ToXContent;
import org.density.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Utility class for converting SearchResponse objects to Protocol Buffers.
 * This class handles the conversion of search operation responses to their
 * Protocol Buffer representation.
 */
public class SearchResponseSectionsProtoUtils {

    private SearchResponseSectionsProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a SearchResponse to its Protocol Buffer representation.
     * Similar to {@link SearchResponseSections#toXContent(XContentBuilder, ToXContent.Params)}
     *
     * @param builder The Protocol Buffer SearchResponse builder to populate
     * @param response The SearchResponse to convert
     * @throws IOException if there's an error during conversion
     */
    protected static void toProto(org.density.protobufs.ResponseBody.Builder builder, SearchResponse response) throws IOException {
        // Convert hits using pass by reference
        org.density.protobufs.HitsMetadata.Builder hitsBuilder = org.density.protobufs.HitsMetadata.newBuilder();
        SearchHitsProtoUtils.toProto(response.getHits(), hitsBuilder);
        builder.setHits(hitsBuilder.build());

        // Check for unsupported features
        checkUnsupportedFeatures(response);
    }

    /**
     * Helper method to check for unsupported features.
     *
     * @param response The SearchResponse to check
     * @throws UnsupportedOperationException if unsupported features are present
     */
    private static void checkUnsupportedFeatures(SearchResponse response) {
        // TODO: Implement aggregations conversion
        if (response.getAggregations() != null) {
            throw new UnsupportedOperationException("aggregation responses are not supported yet");
        }

        // TODO: Implement suggest conversion
        if (response.getSuggest() != null) {
            throw new UnsupportedOperationException("suggest responses are not supported yet");
        }

        // TODO: Implement profile results conversion
        if (response.getProfileResults() != null && !response.getProfileResults().isEmpty()) {
            throw new UnsupportedOperationException("profile results are not supported yet");
        }

        // TODO: Implement search ext builders conversion
        if (response.getInternalResponse().getSearchExtBuilders() != null
            && !response.getInternalResponse().getSearchExtBuilders().isEmpty()) {
            throw new UnsupportedOperationException("ext builder responses are not supported yet");
        }
    }
}
