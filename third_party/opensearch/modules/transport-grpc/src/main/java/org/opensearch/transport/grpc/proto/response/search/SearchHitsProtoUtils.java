/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.density.transport.grpc.proto.response.search;

import org.apache.lucene.search.TotalHits;
import org.density.core.xcontent.ToXContent;
import org.density.core.xcontent.XContentBuilder;
import org.density.protobufs.NullValue;
import org.density.search.SearchHit;
import org.density.search.SearchHits;

import java.io.IOException;

/**
 * Utility class for converting SearchHits objects to Protocol Buffers.
 * This class handles the conversion of search operation responses to their
 * Protocol Buffer representation.
 */
public class SearchHitsProtoUtils {

    private SearchHitsProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a SearchHits to its Protocol Buffer representation.
     * This method is equivalent to {@link SearchHits#toXContent(XContentBuilder, ToXContent.Params)}
     *
     * @param hits The SearchHits to convert
     * @return A Protocol Buffer HitsMetadata representation
     * @throws IOException if there's an error during conversion
     */
    protected static org.density.protobufs.HitsMetadata toProto(SearchHits hits) throws IOException {
        org.density.protobufs.HitsMetadata.Builder hitsMetaData = org.density.protobufs.HitsMetadata.newBuilder();
        toProto(hits, hitsMetaData);
        return hitsMetaData.build();
    }

    /**
     * Converts a SearchHits to its Protocol Buffer representation.
     * This method is equivalent to {@link SearchHits#toXContent(XContentBuilder, ToXContent.Params)}
     *
     * @param hits The SearchHits to convert
     * @param hitsMetaData The builder to populate with the SearchHits data
     * @throws IOException if there's an error during conversion
     */
    protected static void toProto(SearchHits hits, org.density.protobufs.HitsMetadata.Builder hitsMetaData) throws IOException {
        // Process total hits information
        processTotalHits(hits, hitsMetaData);

        // Process max score information
        processMaxScore(hits, hitsMetaData);

        // Process individual hits
        processHits(hits, hitsMetaData);
    }

    /**
     * Helper method to process total hits information.
     *
     * @param hits The SearchHits to process
     * @param hitsMetaData The builder to populate with the total hits data
     */
    private static void processTotalHits(SearchHits hits, org.density.protobufs.HitsMetadata.Builder hitsMetaData) {
        org.density.protobufs.HitsMetadata.Total.Builder totalBuilder = org.density.protobufs.HitsMetadata.Total.newBuilder();

        // TODO need to pass parameters
        // boolean totalHitAsInt = params.paramAsBoolean(RestSearchAction.TOTAL_HITS_AS_INT_PARAM, false);
        boolean totalHitAsInt = false;

        if (totalHitAsInt) {
            long total = hits.getTotalHits() == null ? -1 : hits.getTotalHits().value();
            totalBuilder.setDoubleValue(total);
        } else if (hits.getTotalHits() != null) {
            org.density.protobufs.TotalHits.Builder totalHitsBuilder = org.density.protobufs.TotalHits.newBuilder();
            totalHitsBuilder.setValue(hits.getTotalHits().value());

            // Set relation based on the TotalHits relation
            org.density.protobufs.TotalHits.TotalHitsRelation relation = hits.getTotalHits().relation() == TotalHits.Relation.EQUAL_TO
                ? org.density.protobufs.TotalHits.TotalHitsRelation.TOTAL_HITS_RELATION_EQ
                : org.density.protobufs.TotalHits.TotalHitsRelation.TOTAL_HITS_RELATION_GTE;
            totalHitsBuilder.setRelation(relation);

            totalBuilder.setTotalHits(totalHitsBuilder.build());
        }

        hitsMetaData.setTotal(totalBuilder.build());
    }

    /**
     * Helper method to process max score information.
     *
     * @param hits The SearchHits to process
     * @param hitsMetaData The builder to populate with the max score data
     */
    private static void processMaxScore(SearchHits hits, org.density.protobufs.HitsMetadata.Builder hitsMetaData) {
        org.density.protobufs.HitsMetadata.MaxScore.Builder maxScoreBuilder = org.density.protobufs.HitsMetadata.MaxScore
            .newBuilder();

        if (Float.isNaN(hits.getMaxScore())) {
            maxScoreBuilder.setNullValue(NullValue.NULL_VALUE_NULL);
        } else {
            maxScoreBuilder.setFloatValue(hits.getMaxScore());
        }

        hitsMetaData.setMaxScore(maxScoreBuilder.build());
    }

    /**
     * Helper method to process individual hits.
     *
     * @param hits The SearchHits to process
     * @param hitsMetaData The builder to populate with the hits data
     * @throws IOException if there's an error during conversion
     */
    private static void processHits(SearchHits hits, org.density.protobufs.HitsMetadata.Builder hitsMetaData) throws IOException {
        // Process each hit
        for (SearchHit hit : hits) {
            org.density.protobufs.Hit.Builder hitBuilder = org.density.protobufs.Hit.newBuilder();
            SearchHitProtoUtils.toProto(hit, hitBuilder);
            hitsMetaData.addHits(hitBuilder.build());
        }
    }
}
