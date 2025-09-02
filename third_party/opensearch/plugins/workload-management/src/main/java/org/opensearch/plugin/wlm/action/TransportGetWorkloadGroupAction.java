/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.plugin.wlm.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.density.ResourceNotFoundException;
import org.density.action.support.ActionFilters;
import org.density.action.support.clustermanager.TransportClusterManagerNodeReadAction;
import org.density.cluster.ClusterState;
import org.density.cluster.block.ClusterBlockException;
import org.density.cluster.block.ClusterBlockLevel;
import org.density.cluster.metadata.IndexNameExpressionResolver;
import org.density.cluster.metadata.WorkloadGroup;
import org.density.cluster.service.ClusterService;
import org.density.common.inject.Inject;
import org.density.core.action.ActionListener;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.rest.RestStatus;
import org.density.plugin.wlm.service.WorkloadGroupPersistenceService;
import org.density.search.pipeline.SearchPipelineService;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportService;

import java.io.IOException;
import java.util.Collection;

/**
 * Transport action to get WorkloadGroup
 *
 * @density.experimental
 */
public class TransportGetWorkloadGroupAction extends TransportClusterManagerNodeReadAction<
    GetWorkloadGroupRequest,
    GetWorkloadGroupResponse> {
    private static final Logger logger = LogManager.getLogger(SearchPipelineService.class);

    /**
     * Constructor for TransportGetWorkloadGroupAction
     *
     * @param clusterService - a {@link ClusterService} object
     * @param transportService - a {@link TransportService} object
     * @param actionFilters - a {@link ActionFilters} object
     * @param threadPool - a {@link ThreadPool} object
     * @param indexNameExpressionResolver - a {@link IndexNameExpressionResolver} object
     */
    @Inject
    public TransportGetWorkloadGroupAction(
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            GetWorkloadGroupAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetWorkloadGroupRequest::new,
            indexNameExpressionResolver,
            true
        );
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected GetWorkloadGroupResponse read(StreamInput in) throws IOException {
        return new GetWorkloadGroupResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(GetWorkloadGroupRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected void clusterManagerOperation(
        GetWorkloadGroupRequest request,
        ClusterState state,
        ActionListener<GetWorkloadGroupResponse> listener
    ) throws Exception {
        final String name = request.getName();
        final Collection<WorkloadGroup> resultGroups = WorkloadGroupPersistenceService.getFromClusterStateMetadata(name, state);

        if (resultGroups.isEmpty() && name != null && !name.isEmpty()) {
            logger.warn("No WorkloadGroup exists with the provided name: {}", name);
            throw new ResourceNotFoundException("No WorkloadGroup exists with the provided name: " + name);
        }
        listener.onResponse(new GetWorkloadGroupResponse(resultGroups, RestStatus.OK));
    }
}
