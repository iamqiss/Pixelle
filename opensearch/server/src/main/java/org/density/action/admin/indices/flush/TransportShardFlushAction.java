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

package org.density.action.admin.indices.flush;

import org.density.Version;
import org.density.action.support.ActionFilters;
import org.density.action.support.replication.ReplicationResponse;
import org.density.action.support.replication.TransportReplicationAction;
import org.density.cluster.action.shard.ShardStateAction;
import org.density.cluster.service.ClusterService;
import org.density.common.inject.Inject;
import org.density.common.settings.Settings;
import org.density.core.action.ActionListener;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;
import org.density.core.index.shard.ShardId;
import org.density.index.shard.IndexShard;
import org.density.indices.IndicesService;
import org.density.tasks.Task;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportChannel;
import org.density.transport.TransportRequest;
import org.density.transport.TransportRequestHandler;
import org.density.transport.TransportService;

import java.io.IOException;

/**
 * Transport action for flushing one or more indices
 *
 * @density.internal
 */
public class TransportShardFlushAction extends TransportReplicationAction<ShardFlushRequest, ShardFlushRequest, ReplicationResponse> {

    public static final String NAME = FlushAction.NAME + "[s]";

    @Inject
    public TransportShardFlushAction(
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
            NAME,
            transportService,
            clusterService,
            indicesService,
            threadPool,
            shardStateAction,
            actionFilters,
            ShardFlushRequest::new,
            ShardFlushRequest::new,
            ThreadPool.Names.FLUSH
        );
        transportService.registerRequestHandler(
            PRE_SYNCED_FLUSH_ACTION_NAME,
            ThreadPool.Names.FLUSH,
            PreShardSyncedFlushRequest::new,
            new PreSyncedFlushTransportHandler(indicesService)
        );
    }

    @Override
    protected ReplicationResponse newResponseInstance(StreamInput in) throws IOException {
        return new ReplicationResponse(in);
    }

    @Override
    protected void shardOperationOnPrimary(
        ShardFlushRequest shardRequest,
        IndexShard primary,
        ActionListener<PrimaryResult<ShardFlushRequest, ReplicationResponse>> listener
    ) {
        ActionListener.completeWith(listener, () -> {
            primary.flush(shardRequest.getRequest());
            logger.trace("{} flush request executed on primary", primary.shardId());
            return new PrimaryResult<>(shardRequest, new ReplicationResponse());
        });
    }

    @Override
    protected void shardOperationOnReplica(ShardFlushRequest request, IndexShard replica, ActionListener<ReplicaResult> listener) {
        ActionListener.completeWith(listener, () -> {
            replica.flush(request.getRequest());
            logger.trace("{} flush request executed on replica", replica.shardId());
            return new ReplicaResult();
        });
    }

    // TODO: Remove this transition in Density 3.0
    private static final String PRE_SYNCED_FLUSH_ACTION_NAME = "internal:indices/flush/synced/pre";

    /**
     * A Pre Shard Synced Flush Request
     *
     * @density.internal
     */
    private static class PreShardSyncedFlushRequest extends TransportRequest {
        private final ShardId shardId;

        private PreShardSyncedFlushRequest(StreamInput in) throws IOException {
            super(in);
            assert in.getVersion().before(Version.V_2_0_0) : "received pre_sync request from a new node";
            this.shardId = new ShardId(in);
        }

        @Override
        public String toString() {
            return "PreShardSyncedFlushRequest{" + "shardId=" + shardId + '}';
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            assert false : "must not send pre_sync request from a new node";
            throw new UnsupportedOperationException("");
        }
    }

    /**
     * Pre synced flush handler for the transport layer
     *
     * @density.internal
     */
    private static final class PreSyncedFlushTransportHandler implements TransportRequestHandler<PreShardSyncedFlushRequest> {
        private final IndicesService indicesService;

        PreSyncedFlushTransportHandler(IndicesService indicesService) {
            this.indicesService = indicesService;
        }

        @Override
        public void messageReceived(PreShardSyncedFlushRequest request, TransportChannel channel, Task task) {
            IndexShard indexShard = indicesService.indexServiceSafe(request.shardId.getIndex()).getShard(request.shardId.id());
            indexShard.flush(new FlushRequest().force(false).waitIfOngoing(true));
            throw new UnsupportedOperationException("Synced flush was removed and a normal flush was performed instead.");
        }
    }
}
