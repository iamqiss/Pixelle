/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.indices.streamingingestion.resume;

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
 * Transport action to resume ingestion.
 *
 * @density.experimental
 */
public class TransportResumeIngestionAction extends TransportClusterManagerNodeAction<ResumeIngestionRequest, ResumeIngestionResponse> {

    private static final Logger logger = LogManager.getLogger(TransportResumeIngestionAction.class);

    private final MetadataStreamingIngestionStateService ingestionStateService;
    private final DestructiveOperations destructiveOperations;

    @Inject
    public TransportResumeIngestionAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        MetadataStreamingIngestionStateService ingestionStateService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        DestructiveOperations destructiveOperations
    ) {
        super(
            ResumeIngestionAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            ResumeIngestionRequest::new,
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
    protected ResumeIngestionResponse read(StreamInput in) throws IOException {
        return new ResumeIngestionResponse(in);
    }

    @Override
    protected void doExecute(Task task, ResumeIngestionRequest request, ActionListener<ResumeIngestionResponse> listener) {
        destructiveOperations.failDestructive(request.indices());
        super.doExecute(task, request, listener);
    }

    @Override
    protected ClusterBlockException checkBlock(ResumeIngestionRequest request, ClusterState state) {
        return state.blocks()
            .indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, indexNameExpressionResolver.concreteIndexNames(state, request));
    }

    @Override
    protected void clusterManagerOperation(
        final ResumeIngestionRequest request,
        final ClusterState state,
        final ActionListener<ResumeIngestionResponse> listener
    ) {
        throw new UnsupportedOperationException("The task parameter is required");
    }

    @Override
    protected void clusterManagerOperation(
        final Task task,
        final ResumeIngestionRequest request,
        final ClusterState state,
        final ActionListener<ResumeIngestionResponse> listener
    ) throws Exception {
        final Index[] concreteIndices = indexNameExpressionResolver.concreteIndices(state, request);
        if (concreteIndices == null || concreteIndices.length == 0) {
            listener.onResponse(new ResumeIngestionResponse(true, false, new IngestionStateShardFailure[0], ""));
            return;
        }

        ActionListener<UpdateIngestionStateResponse> stateUpdateListener = new ActionListener<>() {

            @Override
            public void onResponse(UpdateIngestionStateResponse updateIngestionStateResponse) {
                boolean shardsAcked = updateIngestionStateResponse.isAcknowledged() && updateIngestionStateResponse.getFailedShards() == 0;
                ResumeIngestionResponse response = new ResumeIngestionResponse(
                    true,
                    shardsAcked,
                    updateIngestionStateResponse.getShardFailureList(),
                    updateIngestionStateResponse.getErrorMessage()
                );
                listener.onResponse(response);
            }

            @Override
            public void onFailure(Exception e) {
                logger.debug("Error resuming ingestion", e);
                listener.onFailure(e);
            }
        };

        String[] indices = Arrays.stream(concreteIndices).map(Index::getName).toArray(String[]::new);
        if (request.getResetSettings() != null && request.getResetSettings().length > 0) {
            // reset consumer and resume ingestion
            UpdateIngestionStateRequest shardPointerUpdateRequest = getShardPointerUpdateRequest(indices, request);
            UpdateIngestionStateRequest resumeIngestionRequest = getIngestionResumeRequest(indices, request);
            ingestionStateService.resetShardPointerAndResumeIngestion(
                "resume-ingestion",
                concreteIndices,
                shardPointerUpdateRequest,
                resumeIngestionRequest,
                stateUpdateListener
            );
        } else {
            // resume ingestion
            UpdateIngestionStateRequest updateIngestionStateRequest = getIngestionResumeRequest(indices, request);
            ingestionStateService.updateIngestionPollerState(
                "resume-ingestion",
                concreteIndices,
                updateIngestionStateRequest,
                stateUpdateListener
            );
        }
    }

    private UpdateIngestionStateRequest getShardPointerUpdateRequest(String[] indices, ResumeIngestionRequest request) {
        int[] shards = Arrays.stream(request.getResetSettings()).mapToInt(ResumeIngestionRequest.ResetSettings::getShard).toArray();
        UpdateIngestionStateRequest updateIngestionStateRequest = new UpdateIngestionStateRequest(indices, shards);
        updateIngestionStateRequest.timeout(request.clusterManagerNodeTimeout());
        updateIngestionStateRequest.setResetSettings(request.getResetSettings());

        return updateIngestionStateRequest;
    }

    private UpdateIngestionStateRequest getIngestionResumeRequest(String[] indices, ResumeIngestionRequest request) {
        UpdateIngestionStateRequest updateIngestionStateRequest = new UpdateIngestionStateRequest(indices, new int[0]);
        updateIngestionStateRequest.timeout(request.clusterManagerNodeTimeout());
        updateIngestionStateRequest.setIngestionPaused(false);

        return updateIngestionStateRequest;
    }
}
