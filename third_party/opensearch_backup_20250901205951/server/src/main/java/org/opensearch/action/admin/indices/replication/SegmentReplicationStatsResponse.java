/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.indices.replication;

import org.density.action.support.broadcast.BroadcastResponse;
import org.density.common.annotation.PublicApi;
import org.density.core.action.support.DefaultShardOperationFailedException;
import org.density.core.common.Strings;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;
import org.density.core.xcontent.MediaTypeRegistry;
import org.density.core.xcontent.XContentBuilder;
import org.density.index.SegmentReplicationPerGroupStats;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Stats Information regarding the Segment Replication state of indices and their associated shards.
 *
 * @density.api
 */
@PublicApi(since = "1.0.0")
public class SegmentReplicationStatsResponse extends BroadcastResponse {
    private final Map<String, List<SegmentReplicationPerGroupStats>> replicationStats;

    public SegmentReplicationStatsResponse(StreamInput in) throws IOException {
        super(in);
        replicationStats = in.readMapOfLists(StreamInput::readString, SegmentReplicationPerGroupStats::new);
    }

    /**
     * Constructs segment replication stats information for a collection of indices and associated shards. Keeps track of how many total shards
     * were seen, and out of those how many were successfully processed and how many failed.
     *
     * @param totalShards             Total count of shards seen
     * @param successfulShards        Count of shards successfully processed
     * @param failedShards            Count of shards which failed to process
     * @param replicationStats Map of indices to a list of {@link SegmentReplicationPerGroupStats}
     * @param shardFailures           List of failures processing shards
     */
    public SegmentReplicationStatsResponse(
        int totalShards,
        int successfulShards,
        int failedShards,
        Map<String, List<SegmentReplicationPerGroupStats>> replicationStats,
        List<DefaultShardOperationFailedException> shardFailures
    ) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.replicationStats = replicationStats;
    }

    public Map<String, List<SegmentReplicationPerGroupStats>> getReplicationStats() {
        return replicationStats;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (replicationStats.size() > 0) {
            for (String index : replicationStats.keySet()) {
                List<SegmentReplicationPerGroupStats> segmentReplicationStates = replicationStats.get(index);
                if (segmentReplicationStates == null || segmentReplicationStates.size() == 0) {
                    continue;
                }
                builder.startObject(index);
                builder.startArray("primary_stats");
                for (SegmentReplicationPerGroupStats segmentReplicationState : segmentReplicationStates) {
                    builder.startObject();
                    segmentReplicationState.toXContent(builder, params);
                    builder.endObject();
                }
                builder.endArray();
                builder.endObject();
            }
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeMapOfLists(replicationStats, StreamOutput::writeString, (o, v) -> v.writeTo(o));
    }

    @Override
    public String toString() {
        return Strings.toString(MediaTypeRegistry.JSON, this, true, true);
    }
}
