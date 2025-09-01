/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.indices.replication.checkpoint;

import org.density.action.support.replication.ReplicationRequest;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Replication request responsible for publishing checkpoint request to a replica shard.
 *
 * @density.internal
 */
public class PublishCheckpointRequest extends ReplicationRequest<PublishCheckpointRequest> {

    private final ReplicationCheckpoint checkpoint;

    public PublishCheckpointRequest(ReplicationCheckpoint checkpoint) {
        super(checkpoint.getShardId());
        this.checkpoint = checkpoint;
    }

    public PublishCheckpointRequest(StreamInput in) throws IOException {
        super(in);
        this.checkpoint = new ReplicationCheckpoint(in);
    }

    /**
     * Returns Replication Checkpoint
     */
    public ReplicationCheckpoint getCheckpoint() {
        return checkpoint;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        checkpoint.writeTo(out);
    }

    @Override
    public String toString() {
        return "PublishCheckpointRequest{" + "checkpoint=" + checkpoint + '}';
    }
}
