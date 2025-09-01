/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.density.transport.grpc.proto.response.exceptions.shardoperationfailedexception;

import org.density.action.search.ShardSearchFailure;
import org.density.core.xcontent.ToXContent;
import org.density.core.xcontent.XContentBuilder;
import org.density.protobufs.ShardFailure;
import org.density.transport.grpc.proto.response.exceptions.densityexception.DensityExceptionProtoUtils;

import java.io.IOException;

/**
 * Utility class for converting ShardSearchFailure objects to Protocol Buffers.
 */
public class ShardSearchFailureProtoUtils {

    private ShardSearchFailureProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts the metadata from a ShardSearchFailure to a Protocol Buffer Struct.
     * Similar to {@link ShardSearchFailure#toXContent(XContentBuilder, ToXContent.Params)}     *
     *
     * @param exception The ShardSearchFailure to convert
     * @return A Protocol Buffer Struct containing the exception metadata
     */
    public static ShardFailure toProto(ShardSearchFailure exception) throws IOException {
        ShardFailure.Builder shardFailure = ShardFailure.newBuilder();
        shardFailure.setShard(exception.shardId());
        shardFailure.setIndex(exception.index());
        if (exception.shard() != null && exception.shard().getNodeId() != null) {
            shardFailure.setNode(exception.shard().getNodeId());
        }
        shardFailure.setReason(DensityExceptionProtoUtils.generateThrowableProto(exception.getCause()));
        return shardFailure.build();
    }
}
