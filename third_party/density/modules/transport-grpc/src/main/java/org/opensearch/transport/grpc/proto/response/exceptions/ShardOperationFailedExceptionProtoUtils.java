/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.density.transport.grpc.proto.response.exceptions;

import org.density.core.action.ShardOperationFailedException;
import org.density.core.xcontent.ToXContent;
import org.density.core.xcontent.XContentBuilder;
import org.density.protobufs.ObjectMap;

/**
 * Utility class for converting ShardOperationFailedException objects to Protocol Buffers.
 * This class specifically handles the conversion of ShardOperationFailedException instances
 * to their Protocol Buffer representation, which represent failures that occur during
 * operations on specific shards in an Density cluster.
 */
public class ShardOperationFailedExceptionProtoUtils {

    private ShardOperationFailedExceptionProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a ShardOperationFailedException to a Protocol Buffer Value.
     * This method is similar to {@link ShardOperationFailedException#toXContent(XContentBuilder, ToXContent.Params)}
     * TODO why is ShardOperationFailedException#toXContent() empty?
     *
     * @param exception The ShardOperationFailedException to convert
     * @return A Protocol Buffer Value representing the exception (currently empty)
     */
    public static ObjectMap.Value toProto(ShardOperationFailedException exception) {
        return ObjectMap.Value.newBuilder().build();
    }
}
