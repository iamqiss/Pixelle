/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.indices.replication.checkpoint;

import org.density.common.annotation.PublicApi;
import org.density.common.inject.Inject;
import org.density.index.shard.IndexShard;

import java.util.Objects;

/**
 * Publish Segment Replication Checkpoint.
 *
 * @density.api
 */
@PublicApi(since = "2.2.0")
public class SegmentReplicationCheckpointPublisher {

    private final PublishAction publishAction;

    // This Component is behind feature flag so we are manually binding this in IndicesModule.
    @Inject
    public SegmentReplicationCheckpointPublisher(PublishCheckpointAction publishAction) {
        this(publishAction::publish);
    }

    public SegmentReplicationCheckpointPublisher(PublishAction publishAction) {
        this.publishAction = Objects.requireNonNull(publishAction);
    }

    public void publish(IndexShard indexShard, ReplicationCheckpoint checkpoint) {
        publishAction.publish(indexShard, checkpoint);
        indexShard.onCheckpointPublished(checkpoint);
    }

    /**
     * Represents an action that is invoked to publish segment replication checkpoint to replica shard
     *
     * @density.api
     */
    @PublicApi(since = "2.2.0")
    public interface PublishAction {
        void publish(IndexShard indexShard, ReplicationCheckpoint checkpoint);
    }

    /**
     * NoOp Checkpoint publisher
     */
    public static final SegmentReplicationCheckpointPublisher EMPTY = new SegmentReplicationCheckpointPublisher(
        (indexShard, checkpoint) -> {}
    );
}
