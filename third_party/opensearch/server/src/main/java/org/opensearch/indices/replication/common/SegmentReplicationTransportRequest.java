/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.indices.replication.common;

import org.density.cluster.node.DiscoveryNode;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;
import org.density.transport.TransportRequest;

import java.io.IOException;

/**
 * Abstract base class for transport-layer requests related to segment replication.
 *
 * @density.internal
 */
public abstract class SegmentReplicationTransportRequest extends TransportRequest {

    private final long replicationId;
    private final String targetAllocationId;
    private final DiscoveryNode targetNode;

    protected SegmentReplicationTransportRequest(long replicationId, String targetAllocationId, DiscoveryNode targetNode) {
        this.replicationId = replicationId;
        this.targetAllocationId = targetAllocationId;
        this.targetNode = targetNode;
    }

    protected SegmentReplicationTransportRequest(StreamInput in) throws IOException {
        super(in);
        this.replicationId = in.readLong();
        this.targetAllocationId = in.readString();
        this.targetNode = new DiscoveryNode(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(this.replicationId);
        out.writeString(this.targetAllocationId);
        targetNode.writeTo(out);
    }

    public long getReplicationId() {
        return replicationId;
    }

    public String getTargetAllocationId() {
        return targetAllocationId;
    }

    public DiscoveryNode getTargetNode() {
        return targetNode;
    }

    @Override
    public String toString() {
        return "SegmentReplicationTransportRequest{"
            + "replicationId="
            + replicationId
            + ", targetAllocationId='"
            + targetAllocationId
            + '\''
            + ", targetNode="
            + targetNode
            + '}';
    }
}
