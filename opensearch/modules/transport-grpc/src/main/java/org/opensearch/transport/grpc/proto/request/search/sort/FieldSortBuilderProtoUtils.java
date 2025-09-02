/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.transport.grpc.proto.request.search.sort;

import org.density.core.xcontent.XContentParser;
import org.density.protobufs.FieldWithOrderMap;
import org.density.protobufs.ScoreSort;
import org.density.search.sort.FieldSortBuilder;
import org.density.search.sort.SortBuilder;
import org.density.search.sort.SortOrder;

import java.util.List;
import java.util.Map;

import static org.density.transport.grpc.proto.request.search.sort.SortBuilderProtoUtils.fieldOrScoreSort;

/**
 * Utility class for converting FieldSortBuilder components between Density and Protocol Buffers formats.
 * This class provides methods to transform field sort definitions and parameters to ensure proper
 * sorting behavior in search operations.
 */
public class FieldSortBuilderProtoUtils {
    private FieldSortBuilderProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a Protocol Buffer field sort representation to Density SortBuilder objects.
     * Similar to {@link FieldSortBuilder#fromXContent(XContentParser, String)}, this method
     * parses field sort definitions from Protocol Buffers and adds them to the provided list.
     *
     * @param sortBuilder The list of SortBuilder objects to add the parsed field sorts to
     * @param fieldWithOrderMap The Protocol Buffer map containing field names and their sort orders
     */
    public static void fromProto(List<SortBuilder<?>> sortBuilder, FieldWithOrderMap fieldWithOrderMap) {
        for (Map.Entry<String, ScoreSort> entry : fieldWithOrderMap.getFieldWithOrderMapMap().entrySet()) {

            String fieldName = entry.getKey();
            ScoreSort scoreSort = entry.getValue();

            SortOrder order = SortOrderProtoUtils.fromProto(scoreSort.getOrder());

            sortBuilder.add(fieldOrScoreSort(fieldName).order(order));
        }
    }
}
