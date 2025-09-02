/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.cluster.decommission.awareness.delete;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.density.action.support.ActionFilters;
import org.density.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.density.cluster.ClusterState;
import org.density.cluster.block.ClusterBlockException;
import org.density.cluster.block.ClusterBlockLevel;
import org.density.cluster.decommission.DecommissionService;
import org.density.cluster.metadata.IndexNameExpressionResolver;
import org.density.cluster.service.ClusterService;
import org.density.common.inject.Inject;
import org.density.core.action.ActionListener;
import org.density.core.common.io.stream.StreamInput;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportService;

import java.io.IOException;

/**
 * Transport action for delete decommission.
 *
 * @density.internal
 */
public class TransportDeleteDecommissionStateAction extends TransportClusterManagerNodeAction<
    DeleteDecommissionStateRequest,
    DeleteDecommissionStateResponse> {

    private static final Logger logger = LogManager.getLogger(TransportDeleteDecommissionStateAction.class);
    private final DecommissionService decommissionService;

    @Inject
    public TransportDeleteDecommissionStateAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        DecommissionService decommissionService
    ) {
        super(
            DeleteDecommissionStateAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            DeleteDecommissionStateRequest::new,
            indexNameExpressionResolver
        );
        this.decommissionService = decommissionService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected DeleteDecommissionStateResponse read(StreamInput in) throws IOException {
        return new DeleteDecommissionStateResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteDecommissionStateRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void clusterManagerOperation(
        DeleteDecommissionStateRequest request,
        ClusterState state,
        ActionListener<DeleteDecommissionStateResponse> listener
    ) {
        logger.info("Received delete decommission Request [{}]", request);
        this.decommissionService.startRecommissionAction(listener);
    }
}
