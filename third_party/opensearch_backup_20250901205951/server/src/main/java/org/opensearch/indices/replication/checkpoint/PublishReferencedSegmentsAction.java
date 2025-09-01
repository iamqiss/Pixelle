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
import org.density.tasks.Task;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportService;

/**
 * Replication action responsible for publishing referenced segments to a replica shard.
 *
 * @density.api
 */
@ExperimentalApi
public class PublishReferencedSegmentsAction extends AbstractPublishCheckpointAction<
    PublishReferencedSegmentsRequest,
    PublishReferencedSegmentsRequest> {

    public static final String ACTION_NAME = "indices:admin/publish_referenced_segments";
    private static final String TASK_ACTION_NAME = "segrep_publish_referenced_segments";
    protected static Logger logger = LogManager.getLogger(PublishReferencedSegmentsAction.class);

    @Inject
    public PublishReferencedSegmentsAction(
        Settings settings,
        TransportService transportService,
        ClusterService clusterService,
        IndicesService indicesService,
        ThreadPool threadPool,
        ShardStateAction shardStateAction,
        ActionFilters actionFilters
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
            PublishReferencedSegmentsRequest::new,
            PublishReferencedSegmentsRequest::new,
            ThreadPool.Names.GENERIC,
            logger
        );
    }

    @Override
    protected void doExecute(Task task, PublishReferencedSegmentsRequest request, ActionListener<ReplicationResponse> listener) {
        assert false : "use PublishReferencedSegmentsAction#publish";
    }

    /**
     * Publish referenced segment request to shard
     */
    final void publish(IndexShard indexShard, ReferencedSegmentsCheckpoint checkpoint) {
        doPublish(
            indexShard,
            checkpoint,
            new PublishReferencedSegmentsRequest(checkpoint),
            TASK_ACTION_NAME,
            false,
            indexShard.getRecoverySettings().getMergedSegmentReplicationTimeout()
        );
    }

    @Override
    protected void shardOperationOnPrimary(
        PublishReferencedSegmentsRequest request,
        IndexShard primary,
        ActionListener<PrimaryResult<PublishReferencedSegmentsRequest, ReplicationResponse>> listener
    ) {
        ActionListener.completeWith(listener, () -> new PrimaryResult<>(request, new ReplicationResponse()));
    }

    @Override
    protected void doReplicaOperation(PublishReferencedSegmentsRequest request, IndexShard replica) {
        if (request.getReferencedSegmentsCheckpoint().getShardId().equals(replica.shardId())) {
            replica.cleanupRedundantPendingMergeSegment(request.getReferencedSegmentsCheckpoint());
        }
    }
}
