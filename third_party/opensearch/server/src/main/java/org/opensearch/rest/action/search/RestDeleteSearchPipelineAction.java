/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.rest.action.search;

import org.density.action.search.DeleteSearchPipelineRequest;
import org.density.rest.BaseRestHandler;
import org.density.rest.RestRequest;
import org.density.rest.action.RestToXContentListener;
import org.density.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static org.density.rest.RestRequest.Method.DELETE;

/**
 * REST action to delete a search pipeline
 *
 *  @density.internal
 */
public class RestDeleteSearchPipelineAction extends BaseRestHandler {
    @Override
    public String getName() {
        return "search_delete_pipeline_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(DELETE, "/_search/pipeline/{id}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        DeleteSearchPipelineRequest request = new DeleteSearchPipelineRequest(restRequest.param("id"));
        request.clusterManagerNodeTimeout(restRequest.paramAsTime("cluster_manager_timeout", request.clusterManagerNodeTimeout()));
        request.timeout(restRequest.paramAsTime("timeout", request.timeout()));
        return channel -> client.admin().cluster().deleteSearchPipeline(request, new RestToXContentListener<>(channel));
    }
}
