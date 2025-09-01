/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.indices.streamingingestion.state;

import org.apache.lucene.store.AlreadyClosedException;
import org.density.action.admin.indices.streamingingestion.resume.ResumeIngestionRequest;
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
import org.density.core.action.support.DefaultShardOperationFailedException;
import org.density.core.common.io.stream.StreamInput;
import org.density.index.IndexService;
import org.density.index.shard.IndexShard;
import org.density.index.shard.ShardNotFoundException;
import org.density.indices.IndicesService;
import org.density.indices.pollingingest.IngestionSettings;
import org.density.indices.pollingingest.StreamPoller;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportService;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.density.indices.pollingingest.StreamPoller.ResetState.RESET_BY_OFFSET;
import static org.density.indices.pollingingest.StreamPoller.ResetState.RESET_BY_TIMESTAMP;

/**
 * Transport action for updating ingestion state on provided shards. Shard level failures are provided if there are
 * errors during updating shard state.
 *
 * <p>This is for internal use and will not be exposed to the user directly. </p>
 *
 * @density.experimental
 */
public class TransportUpdateIngestionStateAction extends TransportBroadcastByNodeAction<
    UpdateIngestionStateRequest,
    UpdateIngestionStateResponse,
    ShardIngestionState> {

    private final IndicesService indicesService;

    @Inject
    public TransportUpdateIngestionStateAction(
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            UpdateIngestionStateAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            UpdateIngestionStateRequest::new,
            ThreadPool.Names.MANAGEMENT
        );
        this.indicesService = indicesService;
    }

    /**
     * Indicates the shards to consider.
     */
    @Override
    protected ShardsIterator shards(ClusterState clusterState, UpdateIngestionStateRequest request, String[] concreteIndices) {
        Set<Integer> shardSet = Arrays.stream(request.getShards()).boxed().collect(Collectors.toSet());

        Predicate<ShardRouting> shardFilter = ShardRouting::primary;
        if (shardSet.isEmpty() == false) {
            shardFilter = shardFilter.and(shardRouting -> shardSet.contains(shardRouting.shardId().getId()));
        }

        return clusterState.routingTable().allShardsSatisfyingPredicate(request.getIndex(), shardFilter);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, UpdateIngestionStateRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, UpdateIngestionStateRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, request.indices());
    }

    @Override
    protected ShardIngestionState readShardResult(StreamInput in) throws IOException {
        return new ShardIngestionState(in);
    }

    @Override
    protected UpdateIngestionStateResponse newResponse(
        UpdateIngestionStateRequest request,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<ShardIngestionState> responses,
        List<DefaultShardOperationFailedException> shardFailures,
        ClusterState clusterState
    ) {
        return new UpdateIngestionStateResponse(true, totalShards, successfulShards, failedShards, shardFailures);
    }

    @Override
    protected UpdateIngestionStateRequest readRequestFrom(StreamInput in) throws IOException {
        return new UpdateIngestionStateRequest(in);
    }

    /**
     * Updates shard ingestion states depending on the requested changes.
     */
    @Override
    protected ShardIngestionState shardOperation(UpdateIngestionStateRequest request, ShardRouting shardRouting) {
        IndexService indexService = indicesService.indexServiceSafe(shardRouting.shardId().getIndex());
        IndexShard indexShard = indexService.getShard(shardRouting.shardId().id());
        if (indexShard.routingEntry() == null) {
            throw new ShardNotFoundException(indexShard.shardId());
        }

        try {
            // update shard pointer
            if (request.getResetSettings() != null && request.getResetSettings().length > 0) {
                ResumeIngestionRequest.ResetSettings resetSettings = getResetSettingsForShard(request, indexShard);
                StreamPoller.ResetState resetState = getStreamPollerResetState(resetSettings);
                String resetValue = resetSettings != null ? resetSettings.getValue() : null;
                if (resetState != null && resetValue != null) {
                    IngestionSettings ingestionSettings = IngestionSettings.builder()
                        .setResetState(resetState)
                        .setResetValue(resetValue)
                        .build();
                    indexShard.updateShardIngestionState(ingestionSettings);
                }
            }

            // update ingestion state
            if (request.getIngestionPaused() != null) {
                IngestionSettings ingestionSettings = IngestionSettings.builder().setIsPaused(request.getIngestionPaused()).build();
                indexShard.updateShardIngestionState(ingestionSettings);
            }

            return indexShard.getIngestionState();
        } catch (final AlreadyClosedException e) {
            throw new ShardNotFoundException(indexShard.shardId());
        }
    }

    private StreamPoller.ResetState getStreamPollerResetState(ResumeIngestionRequest.ResetSettings resetSettings) {
        if (resetSettings == null || resetSettings.getMode() == null) {
            return null;
        }

        return switch (resetSettings.getMode()) {
            case OFFSET -> RESET_BY_OFFSET;
            case TIMESTAMP -> RESET_BY_TIMESTAMP;
        };
    }

    private ResumeIngestionRequest.ResetSettings getResetSettingsForShard(UpdateIngestionStateRequest request, IndexShard indexShard) {
        ResumeIngestionRequest.ResetSettings[] resetSettings = request.getResetSettings();
        int targetShardId = indexShard.shardId().id();
        return Arrays.stream(resetSettings).filter(setting -> setting.getShard() == targetShardId).findFirst().orElse(null);
    }
}
