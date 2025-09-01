/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.indices.streamingingestion.state;

import org.apache.lucene.store.AlreadyClosedException;
import org.density.DensityException;
import org.density.action.admin.cluster.state.ClusterStateRequest;
import org.density.action.admin.cluster.state.ClusterStateResponse;
import org.density.action.pagination.ShardPaginationStrategy;
import org.density.action.support.ActionFilters;
import org.density.action.support.broadcast.node.TransportBroadcastByNodeAction;
import org.density.cluster.ClusterState;
import org.density.cluster.block.ClusterBlockException;
import org.density.cluster.block.ClusterBlockLevel;
import org.density.cluster.metadata.IndexNameExpressionResolver;
import org.density.cluster.routing.ShardRouting;
import org.density.cluster.routing.ShardsIterator;
import org.density.cluster.service.ClusterService;
import org.density.common.inject.Inject;
import org.density.core.action.ActionListener;
import org.density.core.action.support.DefaultShardOperationFailedException;
import org.density.core.common.io.stream.StreamInput;
import org.density.index.IndexService;
import org.density.index.shard.IndexShard;
import org.density.index.shard.ShardNotFoundException;
import org.density.indices.IndicesService;
import org.density.tasks.Task;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportService;
import org.density.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Transport action for retrieving ingestion state.
 *
 * @density.experimental
 */
public class TransportGetIngestionStateAction extends TransportBroadcastByNodeAction<
    GetIngestionStateRequest,
    GetIngestionStateResponse,
    ShardIngestionState> {

    private final IndicesService indicesService;
    private final ClusterService clusterService;
    private final NodeClient client;

    @Inject
    public TransportGetIngestionStateAction(
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        NodeClient client
    ) {
        super(
            GetIngestionStateAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            GetIngestionStateRequest::new,
            ThreadPool.Names.MANAGEMENT
        );
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.client = client;
    }

    /**
     * Retrieves the cluster state and identifies the (index,shard) pairs to be considered for pagination. Ingestion
     * state is then retrieved the these index and shard pairs.
     */
    @Override
    protected void doExecute(Task task, GetIngestionStateRequest request, ActionListener<GetIngestionStateResponse> listener) {
        if (request.getPageParams() != null) {
            final ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
            clusterStateRequest.setShouldCancelOnTimeout(true);
            clusterStateRequest.setParentTask(client.getLocalNodeId(), task.getId());
            clusterStateRequest.clear().indices(request.indices()).routingTable(true).metadata(true);

            client.admin().cluster().state(clusterStateRequest, new ActionListener<>() {

                @Override
                public void onResponse(ClusterStateResponse clusterStateResponse) {
                    try {
                        executePaginatedGetIngestionAction(task, request, listener, clusterStateResponse);
                    } catch (Exception e) {
                        listener.onFailure(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(new DensityException("Failed to retrieve cluster state", e));
                }
            });
        } else {
            super.doExecute(task, request, listener);
        }
    }

    private void executePaginatedGetIngestionAction(
        Task task,
        GetIngestionStateRequest request,
        ActionListener<GetIngestionStateResponse> listener,
        ClusterStateResponse clusterStateResponse
    ) {
        ShardPaginationStrategy paginationStrategy = new ShardPaginationStrategy(
            request.getPageParams(),
            clusterStateResponse.getState(),
            request.getShards()
        );
        for (ShardRouting shardRouting : paginationStrategy.getRequestedEntities()) {
            // add <index,shard> pairs to be considered for the current page
            request.addIndexShardPair(shardRouting.getIndexName(), shardRouting.getId());
        }

        super.doExecute(task, request, new ActionListener<>() {
            @Override
            public void onResponse(GetIngestionStateResponse getIngestionStateResponse) {
                getIngestionStateResponse.setNextPageToken(paginationStrategy.getResponseToken().getNextToken());
                listener.onResponse(getIngestionStateResponse);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    /**
     * Indicates the shards to consider.
     */
    @Override
    protected ShardsIterator shards(ClusterState clusterState, GetIngestionStateRequest request, String[] concreteIndices) {
        Set<Integer> shardSet = Arrays.stream(request.getShards()).boxed().collect(Collectors.toSet());

        // add filters for index and shard from the request
        Predicate<ShardRouting> shardFilter = ShardRouting::primary;
        if (shardSet.isEmpty() == false) {
            shardFilter = shardFilter.and(shardRouting -> shardSet.contains(shardRouting.shardId().getId()));
        }

        // add filters for index and shard for current page when pagination is enabled
        Map<String, Set<Integer>> indexShardPairsForPage = request.getIndexShardPairsAsMap();
        if (indexShardPairsForPage.isEmpty() == false) {
            shardFilter = shardFilter.and(
                shardRouting -> indexShardPairsForPage.containsKey(shardRouting.getIndexName())
                    && indexShardPairsForPage.get(shardRouting.getIndexName()).contains(shardRouting.getId())
            );
        }

        return clusterState.routingTable().allShardsSatisfyingPredicate(request.indices(), shardFilter);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, GetIngestionStateRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, GetIngestionStateRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ, request.indices());
    }

    @Override
    protected ShardIngestionState readShardResult(StreamInput in) throws IOException {
        return new ShardIngestionState(in);
    }

    @Override
    protected GetIngestionStateResponse newResponse(
        GetIngestionStateRequest request,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<ShardIngestionState> responses,
        List<DefaultShardOperationFailedException> shardFailures,
        ClusterState clusterState
    ) {
        return new GetIngestionStateResponse(
            responses.toArray(new ShardIngestionState[0]),
            totalShards,
            successfulShards,
            failedShards,
            null,
            shardFailures
        );
    }

    @Override
    protected GetIngestionStateRequest readRequestFrom(StreamInput in) throws IOException {
        return new GetIngestionStateRequest(in);
    }

    @Override
    protected ShardIngestionState shardOperation(GetIngestionStateRequest request, ShardRouting shardRouting) {
        IndexService indexService = indicesService.indexServiceSafe(shardRouting.shardId().getIndex());
        IndexShard indexShard = indexService.getShard(shardRouting.shardId().id());
        if (indexShard.routingEntry() == null) {
            throw new ShardNotFoundException(indexShard.shardId());
        }

        try {
            return indexShard.getIngestionState();
        } catch (final AlreadyClosedException e) {
            throw new ShardNotFoundException(indexShard.shardId());
        }
    }
}
