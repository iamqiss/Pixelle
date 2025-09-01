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
import org.density.action.support.replication.ReplicationMode;
import org.density.action.support.replication.ReplicationResponse;
import org.density.cluster.action.shard.ShardStateAction;
import org.density.cluster.service.ClusterService;
import org.density.common.annotation.PublicApi;
import org.density.common.inject.Inject;
import org.density.common.settings.Setting;
import org.density.common.settings.Settings;
import org.density.common.unit.TimeValue;
import org.density.core.action.ActionListener;
import org.density.index.shard.IndexShard;
import org.density.indices.IndicesService;
import org.density.indices.replication.SegmentReplicationTargetService;
import org.density.tasks.Task;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportService;

/**
 * Replication action responsible for publishing checkpoint to a replica shard.
 *
 * @density.api
 */
@PublicApi(since = "2.2.0")
public class PublishCheckpointAction extends AbstractPublishCheckpointAction<PublishCheckpointRequest, PublishCheckpointRequest> {
    private static final String TASK_ACTION_NAME = "segrep_publish_checkpoint";
    public static final String ACTION_NAME = "indices:admin/publishCheckpoint";
    protected static Logger logger = LogManager.getLogger(PublishCheckpointAction.class);

    private final SegmentReplicationTargetService replicationService;

    /**
     * The timeout for retrying publish checkpoint requests.
     */
    public static final Setting<TimeValue> PUBLISH_CHECK_POINT_RETRY_TIMEOUT = Setting.timeSetting(
        "indices.publish_check_point.retry_timeout",
        TimeValue.timeValueMinutes(5),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    @Inject
    public PublishCheckpointAction(
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
            PublishCheckpointRequest::new,
            PublishCheckpointRequest::new,
            ThreadPool.Names.REFRESH,
            logger
        );
        this.replicationService = targetService;
    }

    @Override
    protected Setting<TimeValue> getRetryTimeoutSetting() {
        return PUBLISH_CHECK_POINT_RETRY_TIMEOUT;
    }

    @Override
    protected void doExecute(Task task, PublishCheckpointRequest request, ActionListener<ReplicationResponse> listener) {
        assert false : "use PublishCheckpointAction#publish";
    }

    @Override
    public ReplicationMode getReplicationMode(IndexShard indexShard) {
        return super.getReplicationMode(indexShard);
    }

    /**
     * Publish checkpoint request to shard
     */
    final void publish(IndexShard indexShard, ReplicationCheckpoint checkpoint) {
        doPublish(indexShard, checkpoint, new PublishCheckpointRequest(checkpoint), TASK_ACTION_NAME, false, null);
    }

    @Override
    protected void shardOperationOnPrimary(
        PublishCheckpointRequest request,
        IndexShard primary,
        ActionListener<PrimaryResult<PublishCheckpointRequest, ReplicationResponse>> listener
    ) {
        ActionListener.completeWith(listener, () -> new PrimaryResult<>(request, new ReplicationResponse()));
    }

    @Override
    protected void doReplicaOperation(PublishCheckpointRequest request, IndexShard replica) {
        if (request.getCheckpoint().getShardId().equals(replica.shardId())) {
            replicationService.onNewCheckpoint(request.getCheckpoint(), replica);
        }
    }
}
