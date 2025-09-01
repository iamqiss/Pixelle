/*
 * Copyright Density Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.density.index.shard;

import org.density.core.common.io.stream.StreamInput;
import org.density.core.index.shard.ShardId;

import java.io.IOException;

/**
 * Exception to indicate failures are caused due to the closure of the primary
 * shard.
 *
 * @density.internal
 */
public class PrimaryShardClosedException extends IndexShardClosedException {
    public PrimaryShardClosedException(ShardId shardId) {
        super(shardId, "Primary closed");
    }

    public PrimaryShardClosedException(StreamInput in) throws IOException {
        super(in);
    }
}
