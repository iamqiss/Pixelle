/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright Density Contributors. See
 * GitHub history for details.
 */

package org.density.action.admin.indices.readonly;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.density.action.ActionType;
import org.density.action.support.ActionFilters;
import org.density.action.support.replication.ReplicationOperation;
import org.density.action.support.replication.ReplicationRequest;
import org.density.action.support.replication.ReplicationResponse;
import org.density.action.support.replication.TransportReplicationAction;
import org.density.cluster.action.shard.ShardStateAction;
import org.density.cluster.block.ClusterBlock;
import org.density.cluster.block.ClusterBlocks;
import org.density.cluster.service.ClusterService;
import org.density.common.inject.Inject;
import org.density.common.lease.Releasable;
import org.density.common.settings.Settings;
import org.density.core.action.ActionListener;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;
import org.density.core.index.shard.ShardId;
import org.density.core.tasks.TaskId;
import org.density.index.shard.IndexShard;
import org.density.indices.IndicesService;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportService;

import java.io.IOException;
import java.util.Objects;

/**
 * Action used to verify whether shards have properly applied a given index block,
 * and are no longer executing any operations in violation of that block. This action
 * requests all operation permits of the shard in order to wait for all write operations
 * to complete.
 *
 * @density.internal
 */
public class TransportVerifyShardIndexBlockAction extends TransportReplicationAction<
    TransportVerifyShardIndexBlockAction.ShardRequest,
    TransportVerifyShardIndexBlockAction.ShardRequest,
    ReplicationResponse> {

    public static final String NAME = AddIndexBlockAction.NAME + "[s]";
    public static final ActionType<ReplicationResponse> TYPE = new ActionType<>(NAME, ReplicationResponse::new);
    protected Logger logger = LogManager.getLogger(getClass());

    @Inject
    public TransportVerifyShardIndexBlockAction(
        final Settings settings,
        final TransportService transportService,
        final ClusterService clusterService,
        final IndicesService indicesService,
        final ThreadPool threadPool,
        final ShardStateAction stateAction,
        final ActionFilters actionFilters
    ) {
        super(
            settings,
            NAME,
            transportService,
            clusterService,
            indicesService,
            threadPool,
            stateAction,
            actionFilters,
            ShardRequest::new,
            ShardRequest::new,
            ThreadPool.Names.MANAGEMENT
        );
    }

    @Override
    protected ReplicationResponse newResponseInstance(StreamInput in) throws IOException {
        return new ReplicationResponse(in);
    }

    @Override
    protected void acquirePrimaryOperationPermit(
        final IndexShard primary,
        final ShardRequest request,
        final ActionListener<Releasable> onAcquired
    ) {
        primary.acquireAllPrimaryOperationsPermits(onAcquired, request.timeout());
    }

    @Override
    protected void acquireReplicaOperationPermit(
        final IndexShard replica,
        final ShardRequest request,
        final ActionListener<Releasable> onAcquired,
        final long primaryTerm,
        final long globalCheckpoint,
        final long maxSeqNoOfUpdateOrDeletes
    ) {
        replica.acquireAllReplicaOperationsPermits(primaryTerm, globalCheckpoint, maxSeqNoOfUpdateOrDeletes, onAcquired, request.timeout());
    }

    @Override
    protected void shardOperationOnPrimary(
        final ShardRequest shardRequest,
        final IndexShard primary,
        ActionListener<PrimaryResult<ShardRequest, ReplicationResponse>> listener
    ) {
        ActionListener.completeWith(listener, () -> {
            executeShardOperation(shardRequest, primary);
            return new PrimaryResult<>(shardRequest, new ReplicationResponse());
        });
    }

    @Override
    protected void shardOperationOnReplica(ShardRequest shardRequest, IndexShard replica, ActionListener<ReplicaResult> listener) {
        ActionListener.completeWith(listener, () -> {
            executeShardOperation(shardRequest, replica);
            return new ReplicaResult();
        });
    }

    private void executeShardOperation(final ShardRequest request, final IndexShard indexShard) {
        final ShardId shardId = indexShard.shardId();
        if (indexShard.getActiveOperationsCount() != IndexShard.OPERATIONS_BLOCKED) {
            throw new IllegalStateException(
                "index shard " + shardId + " is not blocking all operations while waiting for block " + request.clusterBlock()
            );
        }

        final ClusterBlocks clusterBlocks = clusterService.state().blocks();
        if (clusterBlocks.hasIndexBlock(shardId.getIndexName(), request.clusterBlock()) == false) {
            throw new IllegalStateException("index shard " + shardId + " has not applied block " + request.clusterBlock());
        }
    }

    @Override
    protected ReplicationOperation.Replicas<ShardRequest> newReplicasProxy() {
        return new VerifyShardReadOnlyActionReplicasProxy();
    }

    /**
     * A {@link ReplicasProxy} that marks as stale the shards that are unavailable during the verification
     * and the flush of the shard. This is done to ensure that such shards won't be later promoted as primary
     * or reopened in an unverified state with potential non flushed translog operations.
     *
     * @density.internal
     */
    class VerifyShardReadOnlyActionReplicasProxy extends ReplicasProxy {
        @Override
        public void markShardCopyAsStaleIfNeeded(
            final ShardId shardId,
            final String allocationId,
            final long primaryTerm,
            final ActionListener<Void> listener
        ) {
            shardStateAction.remoteShardFailed(shardId, allocationId, primaryTerm, true, "mark copy as stale", null, listener);
        }
    }

    /**
     * Per shard request
     *
     * @density.internal
     */
    public static class ShardRequest extends ReplicationRequest<ShardRequest> {

        private final ClusterBlock clusterBlock;

        ShardRequest(StreamInput in) throws IOException {
            super(in);
            clusterBlock = new ClusterBlock(in);
        }

        public ShardRequest(final ShardId shardId, final ClusterBlock clusterBlock, final TaskId parentTaskId) {
            super(shardId);
            this.clusterBlock = Objects.requireNonNull(clusterBlock);
            setParentTask(parentTaskId);
        }

        @Override
        public String toString() {
            return "verify shard " + shardId + " before block with " + clusterBlock;
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            clusterBlock.writeTo(out);
        }

        public ClusterBlock clusterBlock() {
            return clusterBlock;
        }
    }
}
