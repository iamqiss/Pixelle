/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.index;

import org.density.common.Nullable;
import org.density.common.annotation.PublicApi;
import org.density.common.unit.TimeValue;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;
import org.density.core.common.io.stream.Writeable;
import org.density.core.common.unit.ByteSizeValue;
import org.density.core.xcontent.ToXContentFragment;
import org.density.core.xcontent.XContentBuilder;
import org.density.indices.replication.SegmentReplicationState;

import java.io.IOException;

/**
 * SegRep stats for a single shard.
 *
 * @density.api
 */
@PublicApi(since = "2.7.0")
public class SegmentReplicationShardStats implements Writeable, ToXContentFragment {
    private final String allocationId;
    private final long checkpointsBehindCount;
    private final long bytesBehindCount;
    // Total Replication lag observed.
    private final long currentReplicationLagMillis;
    // Total time taken for replicas to catch up. Similar to replication lag except this
    // doesn't include time taken by primary to upload data to remote store.
    private final long currentReplicationTimeMillis;
    private final long lastCompletedReplicationTimeMillis;

    @Nullable
    private SegmentReplicationState currentReplicationState;

    public SegmentReplicationShardStats(
        String allocationId,
        long checkpointsBehindCount,
        long bytesBehindCount,
        long currentReplicationTimeMillis,
        long currentReplicationLagMillis,
        long lastCompletedReplicationTime
    ) {
        this.allocationId = allocationId;
        this.checkpointsBehindCount = checkpointsBehindCount;
        this.bytesBehindCount = bytesBehindCount;
        this.currentReplicationTimeMillis = currentReplicationTimeMillis;
        this.currentReplicationLagMillis = currentReplicationLagMillis;
        this.lastCompletedReplicationTimeMillis = lastCompletedReplicationTime;
    }

    public SegmentReplicationShardStats(StreamInput in) throws IOException {
        this.allocationId = in.readString();
        this.checkpointsBehindCount = in.readVLong();
        this.bytesBehindCount = in.readVLong();
        this.currentReplicationTimeMillis = in.readVLong();
        this.lastCompletedReplicationTimeMillis = in.readVLong();
        this.currentReplicationLagMillis = in.readVLong();
    }

    public String getAllocationId() {
        return allocationId;
    }

    public long getCheckpointsBehindCount() {
        return checkpointsBehindCount;
    }

    public long getBytesBehindCount() {
        return bytesBehindCount;
    }

    public long getCurrentReplicationTimeMillis() {
        return currentReplicationTimeMillis;
    }

    /**
     * Total Replication lag observed.
     * @return currentReplicationLagMillis
     */
    public long getCurrentReplicationLagMillis() {
        return currentReplicationLagMillis;
    }

    /**
     * Total time taken for replicas to catch up. Similar to replication lag except this doesn't include time taken by
     * primary to upload data to remote store.
     * @return lastCompletedReplicationTimeMillis
     */
    public long getLastCompletedReplicationTimeMillis() {
        return lastCompletedReplicationTimeMillis;
    }

    public void setCurrentReplicationState(SegmentReplicationState currentReplicationState) {
        this.currentReplicationState = currentReplicationState;
    }

    @Nullable
    public SegmentReplicationState getCurrentReplicationState() {
        return currentReplicationState;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("allocation_id", allocationId);
        builder.field("checkpoints_behind", checkpointsBehindCount);
        builder.field("bytes_behind", new ByteSizeValue(bytesBehindCount).toString());
        builder.field("current_replication_time", new TimeValue(currentReplicationTimeMillis));
        builder.field("current_replication_lag", new TimeValue(currentReplicationLagMillis));
        builder.field("last_completed_replication_time", new TimeValue(lastCompletedReplicationTimeMillis));
        if (currentReplicationState != null) {
            builder.startObject();
            currentReplicationState.toXContent(builder, params);
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(allocationId);
        out.writeVLong(checkpointsBehindCount);
        out.writeVLong(bytesBehindCount);
        out.writeVLong(currentReplicationTimeMillis);
        out.writeVLong(lastCompletedReplicationTimeMillis);
        out.writeVLong(currentReplicationLagMillis);
    }

    @Override
    public String toString() {
        return "SegmentReplicationShardStats{"
            + "allocationId="
            + allocationId
            + ", checkpointsBehindCount="
            + checkpointsBehindCount
            + ", bytesBehindCount="
            + bytesBehindCount
            + ", currentReplicationLagMillis="
            + currentReplicationLagMillis
            + ", currentReplicationTimeMillis="
            + currentReplicationTimeMillis
            + ", lastCompletedReplicationTimeMillis="
            + lastCompletedReplicationTimeMillis
            + ", currentReplicationState="
            + currentReplicationState
            + '}';
    }
}
