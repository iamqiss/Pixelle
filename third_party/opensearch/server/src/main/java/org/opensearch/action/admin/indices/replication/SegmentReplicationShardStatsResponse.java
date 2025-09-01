/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.indices.replication;

import org.density.common.Nullable;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;
import org.density.core.common.io.stream.Writeable;
import org.density.index.SegmentReplicationPerGroupStats;
import org.density.indices.replication.SegmentReplicationState;

import java.io.IOException;

/**
 * Segment Replication specific response object for fetching stats from either a primary
 * or replica shard. The stats returned are different depending on primary or replica.
 *
 * @density.internal
 */
public class SegmentReplicationShardStatsResponse implements Writeable {

    @Nullable
    private final SegmentReplicationPerGroupStats primaryStats;

    @Nullable
    private final SegmentReplicationState replicaStats;

    public SegmentReplicationShardStatsResponse(StreamInput in) throws IOException {
        this.primaryStats = in.readOptionalWriteable(SegmentReplicationPerGroupStats::new);
        this.replicaStats = in.readOptionalWriteable(SegmentReplicationState::new);
    }

    public SegmentReplicationShardStatsResponse(SegmentReplicationPerGroupStats primaryStats) {
        this.primaryStats = primaryStats;
        this.replicaStats = null;
    }

    public SegmentReplicationShardStatsResponse(SegmentReplicationState replicaStats) {
        this.replicaStats = replicaStats;
        this.primaryStats = null;
    }

    public SegmentReplicationPerGroupStats getPrimaryStats() {
        return primaryStats;
    }

    public SegmentReplicationState getReplicaStats() {
        return replicaStats;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(primaryStats);
        out.writeOptionalWriteable(replicaStats);
    }

    @Override
    public String toString() {
        return "SegmentReplicationShardStatsResponse{" + "primaryStats=" + primaryStats + ", replicaStats=" + replicaStats + '}';
    }
}
