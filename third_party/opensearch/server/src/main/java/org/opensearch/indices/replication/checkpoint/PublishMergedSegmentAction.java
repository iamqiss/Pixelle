/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.indices.replication.checkpoint;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.density.action.support.ActionFilters;
import org.density.action.support.replication.ReplicationResponse;
import org.density.cluster.action.shard.ShardStateAction;
import org.density.cluster.service.ClusterService;
import org.density.common.annotation.ExperimentalApi;
import org.density.common.inject.Inject;
import org.density.common.settings.Settings;
import org.density.core.action.ActionListener;
import org.density.index.shard.IndexShard;
import org.density.indices.IndicesService;
import org.density.indices.replication.SegmentReplicationTargetService;
import org.density.indices.replication.checkpoint.MergedSegmentPublisher.PublishAction;
import org.density.tasks.Task;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportService;

/**
 * Replication action responsible for publishing merged segment to a replica shard.
 *
 * @density.api
 */
@ExperimentalApi
public class PublishMergedSegmentAction extends AbstractPublishCheckpointAction<PublishMergedSegmentRequest, PublishMergedSegmentRequest>
    implements
        PublishAction {
    private static final String TASK_ACTION_NAME = "segrep_publish_merged_segment";
    public static final String ACTION_NAME = "indices:admin/publish_merged_segment";
    protected static Logger logger = LogManager.getLogger(PublishMergedSegmentAction.class);

    private final SegmentReplicationTargetService replicationService;

    @Inject
    public PublishMergedSegmentAction(
        Settings settings,
        TransportService transportService,
        ClusterService clusterService,
        IndicesService indicesService,
        ThreadPool threadPool,
        ShardStateAction shardStateAction,
        ActionFilters actionFilters,
        SegmentReplicationTargetService targetService
    ) {
        super(
            settings,
            ACTION_NAME,
            transportService,
            clusterService,
            indicesService,
            threadPool,
            shardStateAction,
            actionFilters,
            PublishMergedSegmentRequest::new,
            PublishMergedSegmentRequest::new,
            ThreadPool.Names.GENERIC,
            logger
        );
        this.replicationService = targetService;
    }

    @Override
    protected void doExecute(Task task, PublishMergedSegmentRequest request, ActionListener<ReplicationResponse> listener) {
        assert false : "use PublishMergedSegmentAction#publish";
    }

    /**
     * Publish merged segment request to shard
     */
    final public void publish(IndexShard indexShard, MergedSegmentCheckpoint checkpoint) {
        doPublish(
            indexShard,
            checkpoint,
            new PublishMergedSegmentRequest(checkpoint),
            TASK_ACTION_NAME,
            true,
            indexShard.getRecoverySettings().getMergedSegmentReplicationTimeout()
        );
    }

    @Override
    protected void shardOperationOnPrimary(
        PublishMergedSegmentRequest request,
        IndexShard primary,
        ActionListener<PrimaryResult<PublishMergedSegmentRequest, ReplicationResponse>> listener
    ) {
        ActionListener.completeWith(listener, () -> new PrimaryResult<>(request, new ReplicationResponse()));
    }

    @Override
    protected void doReplicaOperation(PublishMergedSegmentRequest request, IndexShard replica) {
        if (request.getMergedSegment().getShardId().equals(replica.shardId())) {
            replicationService.onNewMergedSegmentCheckpoint(request.getMergedSegment(), replica);
        }
    }
}
