/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.cluster.remotestore.stats;

import org.density.action.support.broadcast.BroadcastResponse;
import org.density.common.annotation.PublicApi;
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
 * Remote Store stats response
 *
 * @density.api
 */
@PublicApi(since = "2.8.0")
public class RemoteStoreStatsResponse extends BroadcastResponse {

    private final RemoteStoreStats[] remoteStoreStats;

    public RemoteStoreStatsResponse(StreamInput in) throws IOException {
        super(in);
        remoteStoreStats = in.readArray(RemoteStoreStats::new, RemoteStoreStats[]::new);
    }

    public RemoteStoreStatsResponse(
        RemoteStoreStats[] remoteStoreStats,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<DefaultShardOperationFailedException> shardFailures
    ) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.remoteStoreStats = remoteStoreStats;
    }

    public RemoteStoreStats[] getRemoteStoreStats() {
        return this.remoteStoreStats;
    }

    public Map<String, Map<Integer, List<RemoteStoreStats>>> groupByIndexAndShards() {
        Map<String, Map<Integer, List<RemoteStoreStats>>> indexWiseStats = new HashMap<>();
        for (RemoteStoreStats shardStat : remoteStoreStats) {
            indexWiseStats.computeIfAbsent(shardStat.getShardRouting().getIndexName(), k -> new HashMap<>())
                .computeIfAbsent(shardStat.getShardRouting().getId(), k -> new ArrayList<>())
                .add(shardStat);
        }
        return indexWiseStats;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeArray(remoteStoreStats);
    }

    @Override
    protected void addCustomXContentFields(XContentBuilder builder, Params params) throws IOException {
        Map<String, Map<Integer, List<RemoteStoreStats>>> indexWiseStats = groupByIndexAndShards();
        builder.startObject(Fields.INDICES);
        for (String indexName : indexWiseStats.keySet()) {
            builder.startObject(indexName);
            builder.startObject(Fields.SHARDS);
            for (int shardId : indexWiseStats.get(indexName).keySet()) {
                builder.startArray(Integer.toString(shardId));
                for (RemoteStoreStats shardStat : indexWiseStats.get(indexName).get(shardId)) {
                    shardStat.toXContent(builder, params);
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
