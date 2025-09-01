/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.cluster.shards.routing.weighted.get;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.density.action.support.ActionFilters;
import org.density.action.support.clustermanager.TransportClusterManagerNodeReadAction;
import org.density.cluster.ClusterState;
import org.density.cluster.block.ClusterBlockException;
import org.density.cluster.block.ClusterBlockLevel;
import org.density.cluster.metadata.IndexNameExpressionResolver;
import org.density.cluster.metadata.WeightedRoutingMetadata;
import org.density.cluster.routing.WeightedRouting;
import org.density.cluster.routing.WeightedRoutingService;
import org.density.cluster.service.ClusterService;
import org.density.common.inject.Inject;
import org.density.core.action.ActionListener;
import org.density.core.common.io.stream.StreamInput;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportService;

import java.io.IOException;

/**
 * Transport action for getting weights for weighted round-robin search routing policy
 *
 * @density.internal
 */
public class TransportGetWeightedRoutingAction extends TransportClusterManagerNodeReadAction<
    ClusterGetWeightedRoutingRequest,
    ClusterGetWeightedRoutingResponse> {
    private static final Logger logger = LogManager.getLogger(TransportGetWeightedRoutingAction.class);
    private final WeightedRoutingService weightedRoutingService;

    @Inject
    public TransportGetWeightedRoutingAction(
        TransportService transportService,
        ClusterService clusterService,
        WeightedRoutingService weightedRoutingService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            ClusterGetWeightedRoutingAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            ClusterGetWeightedRoutingRequest::new,
            indexNameExpressionResolver,
            true
        );
        this.weightedRoutingService = weightedRoutingService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ClusterGetWeightedRoutingResponse read(StreamInput in) throws IOException {
        return new ClusterGetWeightedRoutingResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(ClusterGetWeightedRoutingRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected void clusterManagerOperation(
        final ClusterGetWeightedRoutingRequest request,
        ClusterState state,
        final ActionListener<ClusterGetWeightedRoutingResponse> listener
    ) {
        try {
            weightedRoutingService.verifyAwarenessAttribute(request.getAwarenessAttribute());
            WeightedRoutingMetadata weightedRoutingMetadata = state.metadata().custom(WeightedRoutingMetadata.TYPE);
            ClusterGetWeightedRoutingResponse clusterGetWeightedRoutingResponse = new ClusterGetWeightedRoutingResponse();
            if (weightedRoutingMetadata != null && weightedRoutingMetadata.getWeightedRouting() != null) {
                WeightedRouting weightedRouting = weightedRoutingMetadata.getWeightedRouting();
                clusterGetWeightedRoutingResponse = new ClusterGetWeightedRoutingResponse(
                    weightedRouting,
                    state.nodes().getClusterManagerNodeId() != null,
                    weightedRoutingMetadata.getVersion()
                );
            }
            listener.onResponse(clusterGetWeightedRoutingResponse);
        } catch (Exception ex) {
            listener.onFailure(ex);
        }
    }
}
