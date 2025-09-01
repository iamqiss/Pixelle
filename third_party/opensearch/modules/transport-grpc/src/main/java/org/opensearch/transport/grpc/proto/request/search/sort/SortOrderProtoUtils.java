/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.density.transport.grpc.proto.request.search.sort;

import org.density.search.sort.SortOrder;

/**
 * Utility class for converting SortOrder Protocol Buffers to Density objects.
 * This class provides methods to transform Protocol Buffer representations of sort orders
 * from various sort types into their corresponding Density SortOrder enums for search operations.
 */
public class SortOrderProtoUtils {

    private SortOrderProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a Protocol Buffer ScoreSort.SortOrder to an Density SortOrder.
     * Similar to {@link SortOrder#fromString(String)}, this method maps the Protocol Buffer
     * sort order enum values to their corresponding Density SortOrder values.
     *
     * @param sortOrder The Protocol Buffer ScoreSort.SortOrder to convert
     * @return The corresponding Density SortOrder
     * @throws IllegalArgumentException if the sort order is unspecified or invalid
     */
    public static SortOrder fromProto(org.density.protobufs.SortOrder sortOrder) {
        switch (sortOrder) {
            case SORT_ORDER_ASC:
                return SortOrder.ASC;
            case SORT_ORDER_DESC:
                return SortOrder.DESC;
            case SORT_ORDER_UNSPECIFIED:
            default:
                throw new IllegalArgumentException("Must provide oneof sort combinations");
        }
    }

}
