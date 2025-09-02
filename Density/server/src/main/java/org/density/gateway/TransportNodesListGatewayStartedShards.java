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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.density.gateway;

import org.density.DensityException;
import org.density.Version;
import org.density.action.ActionType;
import org.density.action.FailedNodeException;
import org.density.action.support.ActionFilters;
import org.density.action.support.nodes.BaseNodeResponse;
import org.density.action.support.nodes.BaseNodesRequest;
import org.density.action.support.nodes.BaseNodesResponse;
import org.density.action.support.nodes.TransportNodesAction;
import org.density.cluster.ClusterName;
import org.density.cluster.node.DiscoveryNode;
import org.density.cluster.service.ClusterService;
import org.density.common.Nullable;
import org.density.common.inject.Inject;
import org.density.common.settings.Settings;
import org.density.core.action.ActionListener;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;
import org.density.core.index.shard.ShardId;
import org.density.core.xcontent.NamedXContentRegistry;
import org.density.env.NodeEnvironment;
import org.density.gateway.TransportNodesGatewayStartedShardHelper.GatewayStartedShard;
import org.density.indices.IndicesService;
import org.density.indices.replication.checkpoint.ReplicationCheckpoint;
import org.density.indices.store.ShardAttributes;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportRequest;
import org.density.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.density.gateway.TransportNodesGatewayStartedShardHelper.getShardInfoOnLocalNode;

/**
 * This transport action is used to fetch the shard version from each node during primary allocation in {@link GatewayAllocator}.
 * We use this to find out which node holds the latest shard version and which of them used to be a primary in order to allocate
 * shards after node or cluster restarts.
 *
 * @density.internal
 */
public class TransportNodesListGatewayStartedShards extends TransportNodesAction<
    TransportNodesListGatewayStartedShards.Request,
    TransportNodesListGatewayStartedShards.NodesGatewayStartedShards,
    TransportNodesListGatewayStartedShards.NodeRequest,
    TransportNodesListGatewayStartedShards.NodeGatewayStartedShards>
    implements
        AsyncShardFetch.Lister<
            TransportNodesListGatewayStartedShards.NodesGatewayStartedShards,
            TransportNodesListGatewayStartedShards.NodeGatewayStartedShards> {

    public static final String ACTION_NAME = "internal:gateway/local/started_shards";
    public static final ActionType<NodesGatewayStartedShards> TYPE = new ActionType<>(ACTION_NAME, NodesGatewayStartedShards::new);

    private final Settings settings;
    private final NodeEnvironment nodeEnv;
    private final IndicesService indicesService;
    private final NamedXContentRegistry namedXContentRegistry;

    @Inject
    public TransportNodesListGatewayStartedShards(
        Settings settings,
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        NodeEnvironment env,
        IndicesService indicesService,
        NamedXContentRegistry namedXContentRegistry
    ) {
        super(
            ACTION_NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            Request::new,
            NodeRequest::new,
            ThreadPool.Names.FETCH_SHARD_STARTED,
            NodeGatewayStartedShards.class
        );
        this.settings = settings;
        this.nodeEnv = env;
        this.indicesService = indicesService;
        this.namedXContentRegistry = namedXContentRegistry;
    }

    @Override
    public void list(
        Map<ShardId, ShardAttributes> shardAttributesMap,
        DiscoveryNode[] nodes,
        ActionListener<NodesGatewayStartedShards> listener
    ) {
        assert shardAttributesMap.size() == 1 : "only one shard should be specified";
        final ShardId shardId = shardAttributesMap.keySet().iterator().next();
        final String customDataPath = shardAttributesMap.get(shardId).getCustomDataPath();
        execute(new Request(shardId, customDataPath, nodes), listener);
    }

    @Override
    protected NodeRequest newNodeRequest(Request request) {
        return new NodeRequest(request);
    }

    @Override
    protected NodeGatewayStartedShards newNodeResponse(StreamInput in) throws IOException {
        return new NodeGatewayStartedShards(in);
    }

    @Override
    protected NodesGatewayStartedShards newResponse(
        Request request,
        List<NodeGatewayStartedShards> responses,
        List<FailedNodeException> failures
    ) {
        return new NodesGatewayStartedShards(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected NodeGatewayStartedShards nodeOperation(NodeRequest request) {
        try {
            GatewayStartedShard shardInfo = getShardInfoOnLocalNode(
                logger,
                request.getShardId(),
                namedXContentRegistry,
                nodeEnv,
                indicesService,
                request.getCustomDataPath(),
                settings,
                clusterService
            );
            return new NodeGatewayStartedShards(
                clusterService.localNode(),
                new GatewayStartedShard(
                    shardInfo.allocationId(),
                    shardInfo.primary(),
                    shardInfo.replicationCheckpoint(),
                    shardInfo.storeException()
                )
            );
        } catch (Exception e) {
            throw new DensityException("failed to load started shards", e);
        }
    }

    /**
     * The nodes request.
     *
     * @density.internal
     */
    public static class Request extends BaseNodesRequest<Request> {

        private final ShardId shardId;
        @Nullable
        private final String customDataPath;

        public Request(StreamInput in) throws IOException {
            super(in);
            shardId = new ShardId(in);
            customDataPath = in.readString();
        }

        public Request(ShardId shardId, String customDataPath, DiscoveryNode[] nodes) {
            super(nodes);
            this.shardId = Objects.requireNonNull(shardId);
            this.customDataPath = Objects.requireNonNull(customDataPath);
        }

        public ShardId shardId() {
            return shardId;
        }

        /**
         * Returns the custom data path that is used to look up information for this shard.
         * Returns an empty string if no custom data path is used for this index.
         * Returns null if custom data path information is not available (due to BWC).
         */
        @Nullable
        public String getCustomDataPath() {
            return customDataPath;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardId.writeTo(out);
            out.writeString(customDataPath);
        }
    }

    /**
     *  The nodes response.
     *
     * @density.internal
     */
    public static class NodesGatewayStartedShards extends BaseNodesResponse<NodeGatewayStartedShards> {

        public NodesGatewayStartedShards(StreamInput in) throws IOException {
            super(in);
        }

        public NodesGatewayStartedShards(
            ClusterName clusterName,
            List<NodeGatewayStartedShards> nodes,
            List<FailedNodeException> failures
        ) {
            super(clusterName, nodes, failures);
        }

        @Override
        protected List<NodeGatewayStartedShards> readNodesFrom(StreamInput in) throws IOException {
            return in.readList(NodeGatewayStartedShards::new);
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<NodeGatewayStartedShards> nodes) throws IOException {
            out.writeList(nodes);
        }
    }

    /**
     * The request.
     *
     * @density.internal
     */
    public static class NodeRequest extends TransportRequest {

        private final ShardId shardId;
        @Nullable
        private final String customDataPath;

        public NodeRequest(StreamInput in) throws IOException {
            super(in);
            shardId = new ShardId(in);
            customDataPath = in.readString();
        }

        public NodeRequest(Request request) {
            this.shardId = Objects.requireNonNull(request.shardId());
            this.customDataPath = Objects.requireNonNull(request.getCustomDataPath());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardId.writeTo(out);
            assert customDataPath != null;
            out.writeString(customDataPath);
        }

        public ShardId getShardId() {
            return shardId;
        }

        /**
         * Returns the custom data path that is used to look up information for this shard.
         * Returns an empty string if no custom data path is used for this index.
         * Returns null if custom data path information is not available (due to BWC).
         */
        @Nullable
        public String getCustomDataPath() {
            return customDataPath;
        }
    }

    /**
     * The response.
     *
     * @density.internal
     */
    public static class NodeGatewayStartedShards extends BaseNodeResponse {
        private final GatewayStartedShard gatewayStartedShard;

        public NodeGatewayStartedShards(StreamInput in) throws IOException {
            super(in);
            String allocationId = in.readOptionalString();
            boolean primary = in.readBoolean();
            Exception storeException;
            if (in.readBoolean()) {
                storeException = in.readException();
            } else {
                storeException = null;
            }
            ReplicationCheckpoint replicationCheckpoint;
            if (in.getVersion().onOrAfter(Version.V_2_3_0) && in.readBoolean()) {
                replicationCheckpoint = new ReplicationCheckpoint(in);
            } else {
                replicationCheckpoint = null;
            }
            this.gatewayStartedShard = new GatewayStartedShard(allocationId, primary, replicationCheckpoint, storeException);
        }

        public GatewayStartedShard getGatewayShardStarted() {
            return gatewayStartedShard;
        }

        public NodeGatewayStartedShards(DiscoveryNode node, GatewayStartedShard gatewayStartedShard) {
            super(node);
            this.gatewayStartedShard = gatewayStartedShard;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalString(gatewayStartedShard.allocationId());
            out.writeBoolean(gatewayStartedShard.primary());
            if (gatewayStartedShard.storeException() != null) {
                out.writeBoolean(true);
                out.writeException(gatewayStartedShard.storeException());
            } else {
                out.writeBoolean(false);
            }
            if (out.getVersion().onOrAfter(Version.V_2_3_0)) {
                if (gatewayStartedShard.replicationCheckpoint() != null) {
                    out.writeBoolean(true);
                    gatewayStartedShard.replicationCheckpoint().writeTo(out);
                } else {
                    out.writeBoolean(false);
                }
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            NodeGatewayStartedShards that = (NodeGatewayStartedShards) o;

            return gatewayStartedShard.equals(that.gatewayStartedShard);
        }

        @Override
        public int hashCode() {
            return gatewayStartedShard.hashCode();
        }

        @Override
        public String toString() {
            return gatewayStartedShard.toString();
        }
    }
}
