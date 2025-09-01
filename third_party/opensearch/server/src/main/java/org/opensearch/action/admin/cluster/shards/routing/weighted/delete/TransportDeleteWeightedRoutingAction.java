/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.cluster.shards.routing.weighted.delete;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.density.action.support.ActionFilters;
import org.density.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.density.cluster.ClusterState;
import org.density.cluster.block.ClusterBlockException;
import org.density.cluster.block.ClusterBlockLevel;
import org.density.cluster.metadata.IndexNameExpressionResolver;
import org.density.cluster.routing.WeightedRoutingService;
import org.density.cluster.service.ClusterService;
import org.density.common.inject.Inject;
import org.density.core.action.ActionListener;
import org.density.core.common.io.stream.StreamInput;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportService;

import java.io.IOException;

/**
 * Transport action for deleting weights for weighted round-robin search routing policy
 *
 * @density.internal
 */
public class TransportDeleteWeightedRoutingAction extends TransportClusterManagerNodeAction<
    ClusterDeleteWeightedRoutingRequest,
    ClusterDeleteWeightedRoutingResponse> {

    private static final Logger logger = LogManager.getLogger(TransportDeleteWeightedRoutingAction.class);

    private final WeightedRoutingService weightedRoutingService;

    @Inject
    public TransportDeleteWeightedRoutingAction(
        TransportService transportService,
        ClusterService clusterService,
        WeightedRoutingService weightedRoutingService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            ClusterDeleteWeightedRoutingAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            ClusterDeleteWeightedRoutingRequest::new,
            indexNameExpressionResolver
        );
        this.weightedRoutingService = weightedRoutingService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ClusterDeleteWeightedRoutingResponse read(StreamInput in) throws IOException {
        return new ClusterDeleteWeightedRoutingResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(ClusterDeleteWeightedRoutingRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void clusterManagerOperation(
        ClusterDeleteWeightedRoutingRequest request,
        ClusterState state,
        ActionListener<ClusterDeleteWeightedRoutingResponse> listener
    ) throws Exception {
        weightedRoutingService.deleteWeightedRoutingMetadata(request, listener);
    }
}
