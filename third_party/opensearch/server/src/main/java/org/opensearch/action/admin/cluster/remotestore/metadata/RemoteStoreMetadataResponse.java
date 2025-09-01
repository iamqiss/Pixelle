/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.cluster.remotestore.metadata;

import org.density.action.support.broadcast.BroadcastResponse;
import org.density.common.annotation.ExperimentalApi;
import org.density.core.action.support.DefaultShardOperationFailedException;
import org.density.core.common.Strings;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;
import org.density.core.xcontent.MediaTypeRegistry;
import org.density.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Response containing remote store metadata
 *
 * @density.api
 */
@ExperimentalApi
public class RemoteStoreMetadataResponse extends BroadcastResponse {
    private final RemoteStoreShardMetadata[] remoteStoreShardMetadata;

    public RemoteStoreMetadataResponse(StreamInput in) throws IOException {
        super(in);
        remoteStoreShardMetadata = in.readArray(RemoteStoreShardMetadata::new, RemoteStoreShardMetadata[]::new);
    }

    public RemoteStoreMetadataResponse(
        RemoteStoreShardMetadata[] remoteStoreShardMetadata,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<DefaultShardOperationFailedException> shardFailures
    ) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.remoteStoreShardMetadata = remoteStoreShardMetadata;
    }

    /**
     * Groups metadata by index and shard IDs.
     *
     * @return Map of index name to shard ID to list of metadata
     */
    public Map<String, Map<Integer, List<RemoteStoreShardMetadata>>> groupByIndexAndShards() {
        Map<String, Map<Integer, List<RemoteStoreShardMetadata>>> indexWiseMetadata = new HashMap<>();
        for (RemoteStoreShardMetadata metadata : remoteStoreShardMetadata) {
            indexWiseMetadata.computeIfAbsent(metadata.getIndexName(), k -> new HashMap<>())
                .computeIfAbsent(metadata.getShardId(), k -> new ArrayList<>())
                .add(metadata);
        }
        return indexWiseMetadata;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeArray(remoteStoreShardMetadata);
    }

    @Override
    protected void addCustomXContentFields(XContentBuilder builder, Params params) throws IOException {
        Map<String, Map<Integer, List<RemoteStoreShardMetadata>>> indexWiseMetadata = groupByIndexAndShards();
        builder.startObject(Fields.INDICES);
        for (String indexName : indexWiseMetadata.keySet()) {
            builder.startObject(indexName);
            builder.startObject(Fields.SHARDS);
            for (int shardId : indexWiseMetadata.get(indexName).keySet()) {
                builder.startArray(Integer.toString(shardId));
                for (RemoteStoreShardMetadata metadata : indexWiseMetadata.get(indexName).get(shardId)) {
                    metadata.toXContent(builder, params);
                }
                builder.endArray();
            }
            builder.endObject();
            builder.endObject();
        }
        builder.endObject();
    }

    @Override
    public String toString() {
        return Strings.toString(MediaTypeRegistry.JSON, this, true, false);
    }

    static final class Fields {
        static final String SHARDS = "shards";
        static final String INDICES = "indices";
    }
}
