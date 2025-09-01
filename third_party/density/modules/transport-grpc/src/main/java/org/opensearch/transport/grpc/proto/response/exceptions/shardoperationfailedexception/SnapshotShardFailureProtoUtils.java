/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.density.transport.grpc.proto.response.exceptions.shardoperationfailedexception;

import org.density.core.xcontent.ToXContent;
import org.density.core.xcontent.XContentBuilder;
import org.density.protobufs.ShardFailure;
import org.density.snapshots.SnapshotShardFailure;

/**
 * Utility class for converting SnapshotShardFailure objects to Protocol Buffers.
 */
public class SnapshotShardFailureProtoUtils {

    private SnapshotShardFailureProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts the metadata from a SnapshotShardFailure to a Protocol Buffer Struct.
     * Similar to {@link SnapshotShardFailure#toXContent(XContentBuilder, ToXContent.Params)}     *
     *
     * @param exception The SnapshotShardFailure to convert
     * @return A Protocol Buffer Struct containing the exception metadata
     */
    public static ShardFailure toProto(SnapshotShardFailure exception) {
        ShardFailure.Builder shardFailure = ShardFailure.newBuilder();
        shardFailure.setIndex(exception.index());
        // shardFailure.setIndexUuid(exception.index()); // TODO no field called index_uuid in ShardFailure protos
        shardFailure.setShard(exception.shardId());
        // shardFailure.setReason(exception.reason()); // TODO ErrorCause type in ShardFailure, not string
        shardFailure.setIndex(exception.index());
        shardFailure.setNode(exception.nodeId());
        shardFailure.setStatus(exception.status().name());
        return shardFailure.build();
    }
}
