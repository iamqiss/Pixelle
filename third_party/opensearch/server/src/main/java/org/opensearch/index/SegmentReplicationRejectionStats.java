/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.index;

import org.density.Version;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;
import org.density.core.common.io.stream.Writeable;
import org.density.core.xcontent.ToXContentFragment;
import org.density.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Segment replication rejection stats.
 *
 * @density.internal
 */
public class SegmentReplicationRejectionStats implements Writeable, ToXContentFragment {

    /**
     * Total rejections due to segment replication backpressure
     */
    private long totalRejectionCount;

    public SegmentReplicationRejectionStats(final long totalRejectionCount) {
        this.totalRejectionCount = totalRejectionCount;
    }

    public SegmentReplicationRejectionStats(StreamInput in) throws IOException {
        if (in.getVersion().onOrAfter(Version.V_2_12_0)) {
            this.totalRejectionCount = in.readVLong();
        }
    }

    public long getTotalRejectionCount() {
        return totalRejectionCount;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("segment_replication_backpressure");
        builder.field("total_rejected_requests", totalRejectionCount);
        return builder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().onOrAfter(Version.V_2_12_0)) {
            out.writeVLong(totalRejectionCount);
        }
    }

    @Override
    public String toString() {
        return "SegmentReplicationRejectionStats{ totalRejectedRequestCount=" + totalRejectionCount + '}';
    }

}
