/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.cluster.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.density.DensityException;
import org.density.action.admin.indices.streamingingestion.state.TransportUpdateIngestionStateAction;
import org.density.action.admin.indices.streamingingestion.state.UpdateIngestionStateRequest;
import org.density.action.admin.indices.streamingingestion.state.UpdateIngestionStateResponse;
import org.density.cluster.ClusterState;
import org.density.cluster.ClusterStateUpdateTask;
import org.density.cluster.service.ClusterService;
import org.density.common.Priority;
import org.density.common.inject.Inject;
import org.density.common.unit.TimeValue;
import org.density.core.action.ActionListener;
import org.density.core.index.Index;

import java.util.Collections;

/**
 * Service responsible for submitting metadata updates (for example, ingestion pause/resume state change updates).
 *
 * @density.experimental
 */
public class MetadataStreamingIngestionStateService {
    private static final Logger logger = LogManager.getLogger(MetadataStreamingIngestionStateService.class);

    private final ClusterService clusterService;
    private final TransportUpdateIngestionStateAction transportUpdateIngestionStateAction;

    @Inject
    public MetadataStreamingIngestionStateService(
        ClusterService clusterService,
        TransportUpdateIngestionStateAction transportUpdateIngestionStateAction
    ) {
        this.clusterService = clusterService;
        this.transportUpdateIngestionStateAction = transportUpdateIngestionStateAction;
    }

    /**
     *  This method updates the ingestion poller state in two phases for provided index shards.
     *  <ul>
     *      <li>Phase 1: Publishes cluster state update to pause/resume ingestion. This phase finishes once the update is acknowledged</li>
     *      <li>Phase 2: Runs transport action to update cluster state on individual shards and collects success/failure responses.</li>
     *  </ul>
     *
     *  <p> The two phase approach is taken in order to give real time feedback to the user if the ingestion update was a success or failure.
     *  Note that the second phase could be a no-op if the shard already processed the cluster state update.
     */
    public void updateIngestionPollerState(
        String source,
        Index[] concreteIndices,
        UpdateIngestionStateRequest request,
        ActionListener<UpdateIngestionStateResponse> listener
    ) {
        if (concreteIndices == null || concreteIndices.length == 0) {
            throw new IllegalArgumentException("Index is missing");
        }

        if (request.getIngestionPaused() == null) {
            throw new IllegalArgumentException("Ingestion poller target state is missing");
        }

        if (request.getResetSettings() != null && request.getResetSettings().length > 0 && request.getIngestionPaused() != false) {
            throw new IllegalArgumentException("Poller position can only be reset during a resume operation.");
        }

        clusterService.submitStateUpdateTask(source, new ClusterStateUpdateTask(Priority.URGENT) {

            @Override
            public ClusterState execute(ClusterState currentState) {
                return getUpdatedIngestionPausedClusterState(concreteIndices, currentState, request.getIngestionPaused());
            }

            @Override
            public void clusterStateProcessed(final String source, final ClusterState oldState, final ClusterState newState) {
                if (oldState == newState) {
                    logger.debug("Cluster state did not change when trying to set ingestionPaused={}", request.getIngestionPaused());
                    listener.onResponse(new UpdateIngestionStateResponse(false, 0, 0, 0, Collections.emptyList()));
                } else {
                    processUpdateIngestionRequestOnShards(request, new ActionListener<>() {

                        @Override
                        public void onResponse(UpdateIngestionStateResponse updateIngestionStateResponse) {
                            listener.onResponse(updateIngestionStateResponse);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            UpdateIngestionStateResponse response = new UpdateIngestionStateResponse(
                                true,
                                0,
                                0,
                                0,
                                Collections.emptyList()
                            );
                            response.setErrorMessage("Error encountered while verifying ingestion poller state: " + e.getMessage());
                            listener.onResponse(response);
                        }
                    });
                }
            }

            @Override
            public void onFailure(String source, Exception e) {
                listener.onFailure(
                    new DensityException(
                        "Ingestion cluster state update failed to set ingestionPaused={}",
                        request.getIngestionPaused(),
                        e
                    )
                );
            }

            @Override
            public TimeValue timeout() {
                return request.timeout();
            }
        });
    }

    /**
     * Executes transport action to update ingestion state on provided index shards.
     */
    public void processUpdateIngestionRequestOnShards(
        UpdateIngestionStateRequest updateIngestionStateRequest,
        ActionListener<UpdateIngestionStateResponse> listener
    ) {
        transportUpdateIngestionStateAction.execute(updateIngestionStateRequest, listener);
    }

    /**
     * This method resets the consumer and resumes ingestion. This is done in 3 phases.
     * <ol>
     *      <li>First, reset consumer operation is executed.</li>
     *      <li>Second, ingestion state update is started. This then follows the 2 phase approach from {@link #updateIngestionPollerState} to first update
     *      the cluster state followed by synchronously updating each shard of the index</li>
     * </ol>
     *
     * Ingestion state will only be updated if consumer reset is performed successfully.
     */
    public void resetShardPointerAndResumeIngestion(
        String source,
        Index[] concreteIndices,
        UpdateIngestionStateRequest shardPointerUpdateRequest,
        UpdateIngestionStateRequest resumeIngestionRequest,
        ActionListener<UpdateIngestionStateResponse> listener
    ) {
        if (concreteIndices == null || concreteIndices.length == 0) {
            throw new IllegalArgumentException("Index is missing");
        }

        transportUpdateIngestionStateAction.execute(shardPointerUpdateRequest, new ActionListener<>() {

            @Override
            public void onResponse(UpdateIngestionStateResponse updateIngestionStateResponse) {
                boolean isSuccessfulUpdate = updateIngestionStateResponse.isAcknowledged()
                    && updateIngestionStateResponse.getFailedShards() == 0;
                if (isSuccessfulUpdate) {
                    updateIngestionPollerState(source, concreteIndices, resumeIngestionRequest, listener);
                } else {
                    logger.debug("Error resetting consumer pointers");
                    listener.onResponse(updateIngestionStateResponse);
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.debug("Error resetting consumer pointers", e);
                listener.onFailure(e);
            }
        });
    }

    /**
     * Updates ingestionPaused value in provided cluster state.
     */
    private ClusterState getUpdatedIngestionPausedClusterState(
        final Index[] indices,
        final ClusterState currentState,
        boolean ingestionPaused
    ) {
        final Metadata.Builder metadata = Metadata.builder(currentState.metadata());

        for (Index index : indices) {
            final IndexMetadata indexMetadata = metadata.getSafe(index);

            if (indexMetadata.useIngestionSource() == false) {
                logger.debug("Pause/resume request will be ignored for index {} as streaming ingestion is not enabled", index);
            }

            if (indexMetadata.getIngestionStatus().isPaused() != ingestionPaused) {
                IngestionStatus updatedIngestionStatus = new IngestionStatus(ingestionPaused);
                final IndexMetadata.Builder updatedMetadata = IndexMetadata.builder(indexMetadata).ingestionStatus(updatedIngestionStatus);
                metadata.put(updatedMetadata);
            } else {
                logger.debug(
                    "Received request for ingestionPaused:{} for index {}. The state is already ingestionPaused:{}",
                    ingestionPaused,
                    index,
                    ingestionPaused
                );
            }
        }

        return ClusterState.builder(currentState).metadata(metadata).build();
    }
}
