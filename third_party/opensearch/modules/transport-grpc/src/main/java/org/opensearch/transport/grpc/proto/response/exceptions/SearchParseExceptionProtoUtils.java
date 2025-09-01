/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.density.transport.grpc.proto.response.exceptions;

import org.density.core.xcontent.ToXContent;
import org.density.core.xcontent.XContentBuilder;
import org.density.protobufs.ObjectMap;
import org.density.search.SearchParseException;
import org.density.transport.grpc.proto.response.common.ObjectMapProtoUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for converting SearchParseException objects to Protocol Buffers.
 * This class specifically handles the conversion of SearchParseException instances
 * to their Protocol Buffer representation, preserving metadata about search query
 * parsing errors including line and column position information.
 */
public class SearchParseExceptionProtoUtils {

    private SearchParseExceptionProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts the metadata from a SearchParseException to a Protocol Buffer Struct.
     * Similar to {@link SearchParseException#metadataToXContent(XContentBuilder, ToXContent.Params)}
     *
     * @param exception The SearchParseException to convert
     * @return A Protocol Buffer Struct containing the exception metadata
     */
    public static Map<String, ObjectMap.Value> metadataToProto(SearchParseException exception) {
        Map<String, ObjectMap.Value> map = new HashMap<>();
        map.put("line", ObjectMapProtoUtils.toProto(exception.getLineNumber()));
        map.put("col", ObjectMapProtoUtils.toProto(exception.getColumnNumber()));
        return map;
    }
}
