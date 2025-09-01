/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.search;

import org.density.action.support.ActionFilters;
import org.density.action.support.clustermanager.TransportClusterManagerNodeReadAction;
import org.density.cluster.ClusterState;
import org.density.cluster.block.ClusterBlockException;
import org.density.cluster.block.ClusterBlockLevel;
import org.density.cluster.metadata.IndexNameExpressionResolver;
import org.density.cluster.service.ClusterService;
import org.density.common.inject.Inject;
import org.density.core.action.ActionListener;
import org.density.core.common.io.stream.StreamInput;
import org.density.search.pipeline.SearchPipelineService;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportService;

import java.io.IOException;

/**
 * Perform the action of getting a search pipeline
 *
 * @density.internal
 */
public class GetSearchPipelineTransportAction extends TransportClusterManagerNodeReadAction<
    GetSearchPipelineRequest,
    GetSearchPipelineResponse> {

    @Inject
    public GetSearchPipelineTransportAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            GetSearchPipelineAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetSearchPipelineRequest::new,
            indexNameExpressionResolver,
            true
        );
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected GetSearchPipelineResponse read(StreamInput in) throws IOException {
        return new GetSearchPipelineResponse(in);
    }

    @Override
    protected void clusterManagerOperation(
        GetSearchPipelineRequest request,
        ClusterState state,
        ActionListener<GetSearchPipelineResponse> listener
    ) throws Exception {
        listener.onResponse(new GetSearchPipelineResponse(SearchPipelineService.getPipelines(state, request.getIds())));
    }

    @Override
    protected ClusterBlockException checkBlock(GetSearchPipelineRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
