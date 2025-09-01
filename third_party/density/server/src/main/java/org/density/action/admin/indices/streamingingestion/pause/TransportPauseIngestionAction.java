/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.indices.streamingingestion.pause;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.density.action.admin.indices.streamingingestion.IngestionStateShardFailure;
import org.density.action.admin.indices.streamingingestion.state.UpdateIngestionStateRequest;
import org.density.action.admin.indices.streamingingestion.state.UpdateIngestionStateResponse;
import org.density.action.support.ActionFilters;
import org.density.action.support.DestructiveOperations;
import org.density.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.density.cluster.ClusterState;
import org.density.cluster.block.ClusterBlockException;
import org.density.cluster.block.ClusterBlockLevel;
import org.density.cluster.metadata.IndexNameExpressionResolver;
import org.density.cluster.metadata.MetadataStreamingIngestionStateService;
import org.density.cluster.service.ClusterService;
import org.density.common.inject.Inject;
import org.density.core.action.ActionListener;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.index.Index;
import org.density.tasks.Task;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportService;

import java.io.IOException;
import java.util.Arrays;

/**
 * Pause ingestion transport action.
 *
 * @density.experimental
 */
public class TransportPauseIngestionAction extends TransportClusterManagerNodeAction<PauseIngestionRequest, PauseIngestionResponse> {

    private static final Logger logger = LogManager.getLogger(TransportPauseIngestionAction.class);

    private final MetadataStreamingIngestionStateService ingestionStateService;
    private final DestructiveOperations destructiveOperations;

    @Inject
    public TransportPauseIngestionAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        MetadataStreamingIngestionStateService ingestionStateService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        DestructiveOperations destructiveOperations
    ) {
        super(
            PauseIngestionAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PauseIngestionRequest::new,
            indexNameExpressionResolver
        );
        this.ingestionStateService = ingestionStateService;
        this.destructiveOperations = destructiveOperations;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected PauseIngestionResponse read(StreamInput in) throws IOException {
        return new PauseIngestionResponse(in);
    }

    @Override
    protected void doExecute(Task task, PauseIngestionRequest request, ActionListener<PauseIngestionResponse> listener) {
        destructiveOperations.failDestructive(request.indices());
        super.doExecute(task, request, listener);
    }

    @Override
    protected ClusterBlockException checkBlock(PauseIngestionRequest request, ClusterState state) {
        return state.blocks()
            .indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, indexNameExpressionResolver.concreteIndexNames(state, request));
    }

    @Override
    protected void clusterManagerOperation(
        final PauseIngestionRequest request,
        final ClusterState state,
        final ActionListener<PauseIngestionResponse> listener
    ) {
        throw new UnsupportedOperationException("The task parameter is required");
    }

    @Override
    protected void clusterManagerOperation(
        final Task task,
        final PauseIngestionRequest request,
        final ClusterState state,
        final ActionListener<PauseIngestionResponse> listener
    ) throws Exception {
        final Index[] concreteIndices = indexNameExpressionResolver.concreteIndices(state, request);
        if (concreteIndices == null || concreteIndices.length == 0) {
            listener.onResponse(new PauseIngestionResponse(true, false, new IngestionStateShardFailure[0], ""));
            return;
        }

        String[] indices = Arrays.stream(concreteIndices).map(Index::getName).toArray(String[]::new);
        UpdateIngestionStateRequest updateIngestionStateRequest = new UpdateIngestionStateRequest(indices, new int[0]);
        updateIngestionStateRequest.timeout(request.clusterManagerNodeTimeout());
        updateIngestionStateRequest.setIngestionPaused(true);

        ingestionStateService.updateIngestionPollerState(
            "pause-ingestion",
            concreteIndices,
            updateIngestionStateRequest,
            new ActionListener<>() {

                @Override
                public void onResponse(UpdateIngestionStateResponse updateIngestionStateResponse) {
                    boolean shardsAcked = updateIngestionStateResponse.isAcknowledged()
                        && updateIngestionStateResponse.getFailedShards() == 0;
                    PauseIngestionResponse pauseIngestionResponse = new PauseIngestionResponse(
                        true,
                        shardsAcked,
                        updateIngestionStateResponse.getShardFailureList(),
                        updateIngestionStateResponse.getErrorMessage()
                    );
                    listener.onResponse(pauseIngestionResponse);
                }

                @Override
                public void onFailure(Exception e) {
                    logger.debug("Error pausing ingestion", e);
                    listener.onFailure(e);
                }
            }
        );
    }
}
