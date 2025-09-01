/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.transport.grpc.proto.request.document.bulk;

import org.density.action.support.ActiveShardCount;
import org.density.protobufs.WaitForActiveShards;

/**
 * Utility class for handling active shard count settings in gRPC bulk requests.
 * This class provides methods to convert between Protocol Buffer representations
 * and Density ActiveShardCount objects.
 */
public class ActiveShardCountProtoUtils {
    // protected final Settings settings;

    /**
     * Private constructor to prevent instantiation of utility class.
     */
    private ActiveShardCountProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Sets the active shard count on the bulk request based on the protobuf request.
     * Similar to {@link ActiveShardCount#parseString(String)}, this method interprets
     * the wait_for_active_shards parameter from the Protocol Buffer request and applies
     * the appropriate ActiveShardCount setting to the Density bulk request.
     *
     * @param waitForActiveShards The protobuf object containing the active shard count
     * @return The modified bulk request
     */
    public static ActiveShardCount parseProto(WaitForActiveShards waitForActiveShards) {

        switch (waitForActiveShards.getWaitForActiveShardsCase()) {
            case WaitForActiveShards.WaitForActiveShardsCase.WAIT_FOR_ACTIVE_SHARD_OPTIONS:
                switch (waitForActiveShards.getWaitForActiveShardOptions()) {
                    case WAIT_FOR_ACTIVE_SHARD_OPTIONS_UNSPECIFIED:
                        throw new UnsupportedOperationException("No mapping for WAIT_FOR_ACTIVE_SHARD_OPTIONS_UNSPECIFIED");
                    case WAIT_FOR_ACTIVE_SHARD_OPTIONS_ALL:
                        return ActiveShardCount.ALL;
                    default:
                        return ActiveShardCount.DEFAULT;
                }
            case WaitForActiveShards.WaitForActiveShardsCase.INT32_VALUE:
                return ActiveShardCount.from(waitForActiveShards.getInt32Value());
            default:
                return ActiveShardCount.DEFAULT;
        }
    }
}
