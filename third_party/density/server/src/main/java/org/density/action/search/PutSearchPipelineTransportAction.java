/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.search;

import org.density.action.admin.cluster.node.info.NodeInfo;
import org.density.action.admin.cluster.node.info.NodesInfoRequest;
import org.density.action.support.ActionFilters;
import org.density.action.support.clustermanager.AcknowledgedResponse;
import org.density.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.density.cluster.ClusterState;
import org.density.cluster.block.ClusterBlockException;
import org.density.cluster.block.ClusterBlockLevel;
import org.density.cluster.metadata.IndexNameExpressionResolver;
import org.density.cluster.node.DiscoveryNode;
import org.density.common.inject.Inject;
import org.density.core.action.ActionListener;
import org.density.core.common.io.stream.StreamInput;
import org.density.search.pipeline.SearchPipelineInfo;
import org.density.search.pipeline.SearchPipelineService;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportService;
import org.density.transport.client.OriginSettingClient;
import org.density.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.density.search.pipeline.SearchPipelineService.SEARCH_PIPELINE_ORIGIN;

/**
 * Perform the action of putting a search pipeline
 *
 * @density.internal
 */
public class PutSearchPipelineTransportAction extends TransportClusterManagerNodeAction<PutSearchPipelineRequest, AcknowledgedResponse> {

    private final SearchPipelineService searchPipelineService;
    private final OriginSettingClient client;

    @Inject
    public PutSearchPipelineTransportAction(
        ThreadPool threadPool,
        TransportService transportService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        SearchPipelineService searchPipelineService,
        NodeClient client
    ) {
        super(
            PutSearchPipelineAction.NAME,
            transportService,
            searchPipelineService.getClusterService(),
            threadPool,
            actionFilters,
            PutSearchPipelineRequest::new,
            indexNameExpressionResolver
        );
        this.client = new OriginSettingClient(client, SEARCH_PIPELINE_ORIGIN);
        this.searchPipelineService = searchPipelineService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected AcknowledgedResponse read(StreamInput in) throws IOException {
        return new AcknowledgedResponse(in);
    }

    @Override
    protected void clusterManagerOperation(
        PutSearchPipelineRequest request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) throws Exception {
        NodesInfoRequest nodesInfoRequest = new NodesInfoRequest().clear().addMetric(NodesInfoRequest.Metric.SEARCH_PIPELINES.metricName());
        client.admin().cluster().nodesInfo(nodesInfoRequest, ActionListener.wrap(nodeInfos -> {
            Map<DiscoveryNode, SearchPipelineInfo> searchPipelineInfos = new HashMap<>();
            for (NodeInfo nodeInfo : nodeInfos.getNodes()) {
                searchPipelineInfos.put(nodeInfo.getNode(), nodeInfo.getInfo(SearchPipelineInfo.class));
            }
            searchPipelineService.putPipeline(searchPipelineInfos, request, listener);
        }, listener::onFailure));
    }

    @Override
    protected ClusterBlockException checkBlock(PutSearchPipelineRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
