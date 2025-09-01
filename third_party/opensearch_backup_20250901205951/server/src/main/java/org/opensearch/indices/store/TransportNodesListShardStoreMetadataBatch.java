/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.indices.store;

import org.density.DensityException;
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
import org.density.common.inject.Inject;
import org.density.common.settings.Settings;
import org.density.core.action.ActionListener;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;
import org.density.core.index.shard.ShardId;
import org.density.env.NodeEnvironment;
import org.density.gateway.AsyncShardFetch;
import org.density.index.store.Store;
import org.density.indices.IndicesService;
import org.density.indices.store.TransportNodesListShardStoreMetadataHelper.StoreFilesMetadata;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportRequest;
import org.density.transport.TransportService;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.density.indices.store.TransportNodesListShardStoreMetadataHelper.INDEX_NOT_FOUND;

/**
 * Transport action for fetching the batch of shard stores Metadata from a list of transport nodes
 *
 * @density.internal
 */
public class TransportNodesListShardStoreMetadataBatch extends TransportNodesAction<
    TransportNodesListShardStoreMetadataBatch.Request,
    TransportNodesListShardStoreMetadataBatch.NodesStoreFilesMetadataBatch,
    TransportNodesListShardStoreMetadataBatch.NodeRequest,
    TransportNodesListShardStoreMetadataBatch.NodeStoreFilesMetadataBatch>
    implements
        AsyncShardFetch.Lister<
            TransportNodesListShardStoreMetadataBatch.NodesStoreFilesMetadataBatch,
            TransportNodesListShardStoreMetadataBatch.NodeStoreFilesMetadataBatch> {

    public static final String ACTION_NAME = "internal:cluster/nodes/indices/shard/store/batch";
    public static final ActionType<TransportNodesListShardStoreMetadataBatch.NodesStoreFilesMetadataBatch> TYPE = new ActionType<>(
        ACTION_NAME,
        TransportNodesListShardStoreMetadataBatch.NodesStoreFilesMetadataBatch::new
    );

    private final Settings settings;
    private final IndicesService indicesService;
    private final NodeEnvironment nodeEnv;

    @Inject
    public TransportNodesListShardStoreMetadataBatch(
        Settings settings,
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        NodeEnvironment nodeEnv,
        ActionFilters actionFilters
    ) {
        super(
            ACTION_NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            Request::new,
            NodeRequest::new,
            ThreadPool.Names.FETCH_SHARD_STORE,
            NodeStoreFilesMetadataBatch.class
        );
        this.settings = settings;
        this.indicesService = indicesService;
        this.nodeEnv = nodeEnv;
    }

    @Override
    public void list(
        Map<ShardId, ShardAttributes> shardAttributes,
        DiscoveryNode[] nodes,
        ActionListener<NodesStoreFilesMetadataBatch> listener
    ) {
        execute(new TransportNodesListShardStoreMetadataBatch.Request(shardAttributes, nodes), listener);
    }

    @Override
    protected NodeRequest newNodeRequest(Request request) {
        return new NodeRequest(request);
    }

    @Override
    protected NodeStoreFilesMetadataBatch newNodeResponse(StreamInput in) throws IOException {
        return new NodeStoreFilesMetadataBatch(in);
    }

    @Override
    protected NodesStoreFilesMetadataBatch newResponse(
        Request request,
        List<NodeStoreFilesMetadataBatch> responses,
        List<FailedNodeException> failures
    ) {
        return new NodesStoreFilesMetadataBatch(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected NodeStoreFilesMetadataBatch nodeOperation(NodeRequest request) {
        try {
            return new NodeStoreFilesMetadataBatch(clusterService.localNode(), listStoreMetadata(request));
        } catch (IOException e) {
            throw new DensityException(
                "Failed to list store metadata for shards [" + request.getShardAttributes().keySet().stream().map(ShardId::toString) + "]",
                e
            );
        }
    }

    /**
     * This method is similar to listStoreMetadata method of {@link TransportNodesListShardStoreMetadata}
     * In this case we fetch the shard store files for batch of shards instead of one shard.
     */
    private Map<ShardId, NodeStoreFilesMetadata> listStoreMetadata(NodeRequest request) throws IOException {
        Map<ShardId, NodeStoreFilesMetadata> shardStoreMetadataMap = new HashMap<ShardId, NodeStoreFilesMetadata>();
        for (Map.Entry<ShardId, ShardAttributes> shardAttributes : request.getShardAttributes().entrySet()) {
            final ShardId shardId = shardAttributes.getKey();
            try {
                StoreFilesMetadata storeFilesMetadata = TransportNodesListShardStoreMetadataHelper.listShardMetadataInternal(
                    logger,
                    shardId,
                    nodeEnv,
                    indicesService,
                    shardAttributes.getValue().getCustomDataPath(),
                    settings,
                    clusterService
                );
                shardStoreMetadataMap.put(shardId, new NodeStoreFilesMetadata(storeFilesMetadata, null));
            } catch (Exception e) {
                // should return null in case of known exceptions being returned from listShardMetadataInternal method.
                if (e.getMessage().contains(INDEX_NOT_FOUND) || e instanceof IOException) {
                    shardStoreMetadataMap.put(shardId, null);
                } else {
                    // return actual exception as it is for unknown exceptions
                    shardStoreMetadataMap.put(
                        shardId,
                        new NodeStoreFilesMetadata(
                            new StoreFilesMetadata(shardId, Store.MetadataSnapshot.EMPTY, Collections.emptyList()),
                            e
                        )
                    );
                }
            }
        }
        return shardStoreMetadataMap;
    }

    /**
     * Request is used in constructing the request for making the transport request to set of other node.
     * Refer {@link TransportNodesAction} class start method.
     *
     * @density.internal
     */
    public static class Request extends BaseNodesRequest<Request> {

        private final Map<ShardId, ShardAttributes> shardAttributes;

        public Request(StreamInput in) throws IOException {
            super(in);
            shardAttributes = in.readMap(ShardId::new, ShardAttributes::new);
        }

        public Request(Map<ShardId, ShardAttributes> shardAttributes, DiscoveryNode[] nodes) {
            super(nodes);
            this.shardAttributes = Objects.requireNonNull(shardAttributes);
        }

        public Map<ShardId, ShardAttributes> getShardAttributes() {
            return shardAttributes;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeMap(shardAttributes, (o, k) -> k.writeTo(o), (o, v) -> v.writeTo(o));
        }
    }

    /**
     * Metadata for the nodes store files
     *
     * @density.internal
     */
    public static class NodesStoreFilesMetadataBatch extends BaseNodesResponse<NodeStoreFilesMetadataBatch> {

        public NodesStoreFilesMetadataBatch(StreamInput in) throws IOException {
            super(in);
        }

        public NodesStoreFilesMetadataBatch(
            ClusterName clusterName,
            List<NodeStoreFilesMetadataBatch> nodes,
            List<FailedNodeException> failures
        ) {
            super(clusterName, nodes, failures);
        }

        @Override
        protected List<NodeStoreFilesMetadataBatch> readNodesFrom(StreamInput in) throws IOException {
            return in.readList(NodeStoreFilesMetadataBatch::new);
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<NodeStoreFilesMetadataBatch> nodes) throws IOException {
            out.writeList(nodes);
        }
    }

    /**
     * The metadata for the node store files
     *
     * @density.internal
     */
    public static class NodeStoreFilesMetadata {

        private StoreFilesMetadata storeFilesMetadata;
        private Exception storeFileFetchException;

        public NodeStoreFilesMetadata(StoreFilesMetadata storeFilesMetadata) {
            this.storeFilesMetadata = storeFilesMetadata;
            this.storeFileFetchException = null;
        }

        public NodeStoreFilesMetadata(StreamInput in) throws IOException {
            storeFilesMetadata = new StoreFilesMetadata(in);
            if (in.readBoolean()) {
                this.storeFileFetchException = in.readException();
            } else {
                this.storeFileFetchException = null;
            }
        }

        public NodeStoreFilesMetadata(StoreFilesMetadata storeFilesMetadata, Exception storeFileFetchException) {
            this.storeFilesMetadata = storeFilesMetadata;
            this.storeFileFetchException = storeFileFetchException;
        }

        public StoreFilesMetadata storeFilesMetadata() {
            return storeFilesMetadata;
        }

        public void writeTo(StreamOutput out) throws IOException {
            storeFilesMetadata.writeTo(out);
            if (storeFileFetchException != null) {
                out.writeBoolean(true);
                out.writeException(storeFileFetchException);
            } else {
                out.writeBoolean(false);
            }
        }

        public static boolean isEmpty(NodeStoreFilesMetadata response) {
            return response.storeFilesMetadata() == null
                || response.storeFilesMetadata().isEmpty() && response.getStoreFileFetchException() == null;
        }

        public Exception getStoreFileFetchException() {
            return storeFileFetchException;
        }

        @Override
        public String toString() {
            return "[[" + storeFilesMetadata + "]]";
        }
    }

    /**
     * NodeRequest class is for deserializing the  request received by this node from other node for this transport action.
     * This is used in {@link TransportNodesAction}
     * @density.internal
     */
    public static class NodeRequest extends TransportRequest {

        private final Map<ShardId, ShardAttributes> shardAttributes;

        public NodeRequest(StreamInput in) throws IOException {
            super(in);
            shardAttributes = in.readMap(ShardId::new, ShardAttributes::new);
        }

        public NodeRequest(Request request) {
            this.shardAttributes = Objects.requireNonNull(request.getShardAttributes());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeMap(shardAttributes, (o, k) -> k.writeTo(o), (o, v) -> v.writeTo(o));
        }

        public Map<ShardId, ShardAttributes> getShardAttributes() {
            return shardAttributes;
        }
    }

    /**
     * NodeStoreFilesMetadataBatch Response received by the node from other node for this transport action.
     * Refer {@link TransportNodesAction}
     */
    public static class NodeStoreFilesMetadataBatch extends BaseNodeResponse {
        private final Map<ShardId, NodeStoreFilesMetadata> nodeStoreFilesMetadataBatch;

        protected NodeStoreFilesMetadataBatch(StreamInput in) throws IOException {
            super(in);
            this.nodeStoreFilesMetadataBatch = in.readMap(ShardId::new, i -> {
                if (i.readBoolean()) {
                    return new NodeStoreFilesMetadata(i);
                } else {
                    return null;
                }
            });
        }

        public NodeStoreFilesMetadataBatch(DiscoveryNode node, Map<ShardId, NodeStoreFilesMetadata> nodeStoreFilesMetadataBatch) {
            super(node);
            this.nodeStoreFilesMetadataBatch = nodeStoreFilesMetadataBatch;
        }

        public Map<ShardId, NodeStoreFilesMetadata> getNodeStoreFilesMetadataBatch() {
            return this.nodeStoreFilesMetadataBatch;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeMap(nodeStoreFilesMetadataBatch, (o, k) -> k.writeTo(o), (o, v) -> {
                if (v != null) {
                    o.writeBoolean(true);
                    v.writeTo(o);
                } else {
                    o.writeBoolean(false);
                }
            });
        }
    }

}
