/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.cluster.decommission.awareness.put;

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
 * Transport action for registering decommission
 *
 * @density.internal
 */
public class TransportDecommissionAction extends TransportClusterManagerNodeAction<DecommissionRequest, DecommissionResponse> {

    private static final Logger logger = LogManager.getLogger(TransportDecommissionAction.class);
    private final DecommissionService decommissionService;

    @Inject
    public TransportDecommissionAction(
        TransportService transportService,
        ClusterService clusterService,
        DecommissionService decommissionService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            DecommissionAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            DecommissionRequest::new,
            indexNameExpressionResolver
        );
        this.decommissionService = decommissionService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected DecommissionResponse read(StreamInput in) throws IOException {
        return new DecommissionResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(DecommissionRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void clusterManagerOperation(DecommissionRequest request, ClusterState state, ActionListener<DecommissionResponse> listener)
        throws Exception {
        logger.info("starting awareness attribute [{}] decommissioning", request.getDecommissionAttribute().toString());
        decommissionService.startDecommissionAction(request, listener);
    }
}
