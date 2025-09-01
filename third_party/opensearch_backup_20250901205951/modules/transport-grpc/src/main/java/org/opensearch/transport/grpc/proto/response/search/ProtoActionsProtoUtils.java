/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.transport.grpc.proto.response.search;

import org.density.core.action.ShardOperationFailedException;
import org.density.core.xcontent.ToXContent;
import org.density.core.xcontent.XContentBuilder;
import org.density.protobufs.ResponseBody;
import org.density.rest.action.RestActions;

import java.io.IOException;

/**
 * Utility class for converting REST-like actions between Density and Protocol Buffers formats.
 * This class provides methods to transform response components such as shard statistics and
 * broadcast headers to ensure proper communication between the Density server and gRPC clients.
 */
public class ProtoActionsProtoUtils {

    private ProtoActionsProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Similar to {@link RestActions#buildBroadcastShardsHeader(XContentBuilder, ToXContent.Params, int, int, int, int, ShardOperationFailedException[])}
     *
     * @param searchResponseBodyProtoBuilder the response body builder to populate with shard statistics
     * @param total the total number of shards
     * @param successful the number of successful shards
     * @param skipped the number of skipped shards
     * @param failed the number of failed shards
     * @param shardFailures the array of shard operation failures
     * @throws IOException if there's an error during conversion
     */
    protected static void buildBroadcastShardsHeader(
        ResponseBody.Builder searchResponseBodyProtoBuilder,
        int total,
        int successful,
        int skipped,
        int failed,
        ShardOperationFailedException[] shardFailures
    ) throws IOException {
        searchResponseBodyProtoBuilder.setShards(
            ShardStatisticsProtoUtils.getShardStats(total, successful, skipped, failed, shardFailures)
        );
    }
}
