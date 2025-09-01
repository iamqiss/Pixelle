/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.indices.replication.checkpoint;

import org.density.common.annotation.ExperimentalApi;
import org.density.common.inject.Inject;
import org.density.index.shard.IndexShard;

import java.util.Objects;

/**
 * Publish merged segment.
 *
 * @density.api
 */
@ExperimentalApi
public class MergedSegmentPublisher {
    private final PublishAction publishAction;

    // This Component is behind feature flag so we are manually binding this in IndicesModule.
    @Inject
    public MergedSegmentPublisher(PublishAction publishAction) {
        this.publishAction = Objects.requireNonNull(publishAction);
    }

    public void publish(IndexShard indexShard, MergedSegmentCheckpoint checkpoint) {
        publishAction.publish(indexShard, checkpoint);
    }

    /**
     * Represents an action that is invoked to publish merged segment to replica shard
     *
     * @density.api
     */
    @ExperimentalApi
    public interface PublishAction {
        void publish(IndexShard indexShard, MergedSegmentCheckpoint checkpoint);
    }

    /**
     * NoOp Checkpoint publisher
     */
    public static final MergedSegmentPublisher EMPTY = new MergedSegmentPublisher((indexShard, checkpoint) -> {});
}
