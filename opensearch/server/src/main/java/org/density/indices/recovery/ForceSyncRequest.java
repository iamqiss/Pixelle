/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.indices.recovery;

import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;
import org.density.core.index.shard.ShardId;

import java.io.IOException;

/**
 * Request to force a round of segment replication on primary target
 *
 * @density.internal
 */
public class ForceSyncRequest extends RecoveryTransportRequest {
    private final long recoveryId;
    private final ShardId shardId;

    public ForceSyncRequest(long requestSeqNo, long recoveryId, ShardId shardId) {
        super(requestSeqNo);
        this.recoveryId = recoveryId;
        this.shardId = shardId;
    }

    public ForceSyncRequest(StreamInput in) throws IOException {
        super(in);
        this.recoveryId = in.readLong();
        this.shardId = new ShardId(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(recoveryId);
        shardId.writeTo(out);
    }

    public long getRecoveryId() {
        return recoveryId;
    }

    public ShardId getShardId() {
        return shardId;
    }
}
