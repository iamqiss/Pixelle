/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.indices.replication.common;

import org.density.DensityException;
import org.density.common.Nullable;
import org.density.common.annotation.PublicApi;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.index.shard.ShardId;
import org.density.index.shard.IndexShard;

import java.io.IOException;

/**
 * Exception thrown if replication fails
 *
 * @density.api
 */
@PublicApi(since = "2.2.0")
public class ReplicationFailedException extends DensityException {

    public ReplicationFailedException(IndexShard shard, Throwable cause) {
        this(shard, null, cause);
    }

    public ReplicationFailedException(IndexShard shard, @Nullable String extraInfo, Throwable cause) {
        this(shard.shardId(), extraInfo, cause);
    }

    public ReplicationFailedException(ShardId shardId, @Nullable String extraInfo, Throwable cause) {
        super(shardId + ": Replication failed on " + (extraInfo == null ? "" : " (" + extraInfo + ")"), cause);
    }

    public ReplicationFailedException(StreamInput in) throws IOException {
        super(in);
    }

    public ReplicationFailedException(Exception e) {
        super(e);
    }

    public ReplicationFailedException(String msg) {
        super(msg);
    }

    public ReplicationFailedException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
