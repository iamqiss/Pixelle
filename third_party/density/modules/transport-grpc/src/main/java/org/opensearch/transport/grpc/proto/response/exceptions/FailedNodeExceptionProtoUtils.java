/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.density.transport.grpc.proto.response.exceptions;

import org.density.action.FailedNodeException;
import org.density.core.xcontent.ToXContent;
import org.density.core.xcontent.XContentBuilder;
import org.density.protobufs.ObjectMap;
import org.density.transport.grpc.proto.response.common.ObjectMapProtoUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for converting FailedNodeException objects to Protocol Buffers.
 * This class specifically handles the conversion of FailedNodeException instances
 * to their Protocol Buffer representation, preserving metadata about node failures
 * in a distributed Density cluster.
 */
public class FailedNodeExceptionProtoUtils {

    private FailedNodeExceptionProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts the metadata from a FailedNodeException to a Protocol Buffer Struct.
     * Similar to {@link FailedNodeException#metadataToXContent(XContentBuilder, ToXContent.Params)}     *
     *
     * @param exception The FailedNodeException to convert
     * @return A Protocol Buffer Struct containing the exception metadata
     */
    public static Map<String, ObjectMap.Value> metadataToProto(FailedNodeException exception) {
        Map<String, ObjectMap.Value> map = new HashMap<>();
        map.put("node_id", ObjectMapProtoUtils.toProto(exception.nodeId()));
        return map;
    }
}
