/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.density.transport.grpc.proto.response.exceptions.shardoperationfailedexception;

import org.density.action.search.ShardSearchFailure;
import org.density.action.support.replication.ReplicationResponse;
import org.density.core.action.ShardOperationFailedException;
import org.density.core.action.support.DefaultShardOperationFailedException;
import org.density.core.xcontent.ToXContent;
import org.density.core.xcontent.XContentBuilder;
import org.density.protobufs.ShardFailure;
import org.density.snapshots.SnapshotShardFailure;

import java.io.IOException;

/**
 * Utility class for converting ShardOperationFailedException objects to Protocol Buffers.
 */
public class ShardOperationFailedExceptionProtoUtils {

    private ShardOperationFailedExceptionProtoUtils() {
        // Utility class, no instances
    }

    /**
     * This method is similar to {@link org.density.core.action.ShardOperationFailedException#toXContent(XContentBuilder, ToXContent.Params)}
     * This method is overridden by various exception classes, which are hardcoded here.
     *
     * @param exception The ShardOperationFailedException to convert metadata from
     * @return ShardFailure
     */
    public static ShardFailure toProto(ShardOperationFailedException exception) throws IOException {
        if (exception instanceof ShardSearchFailure) {
            return ShardSearchFailureProtoUtils.toProto((ShardSearchFailure) exception);
        } else if (exception instanceof SnapshotShardFailure) {
            return SnapshotShardFailureProtoUtils.toProto((SnapshotShardFailure) exception);
        } else if (exception instanceof DefaultShardOperationFailedException) {
            return DefaultShardOperationFailedExceptionProtoUtils.toProto((DefaultShardOperationFailedException) exception);
        } else if (exception instanceof ReplicationResponse.ShardInfo.Failure) {
            return ReplicationResponseShardInfoFailureProtoUtils.toProto((ReplicationResponse.ShardInfo.Failure) exception);
        } else {
            throw new UnsupportedOperationException(
                "Unsupported ShardOperationFailedException " + exception.getClass().getName() + "cannot be converted to proto."
            );
        }
    }
}
