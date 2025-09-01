/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.rest.action.search;

import org.density.action.search.GetSearchPipelineRequest;
import org.density.core.common.Strings;
import org.density.rest.BaseRestHandler;
import org.density.rest.RestRequest;
import org.density.rest.action.RestStatusToXContentListener;
import org.density.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static org.density.rest.RestRequest.Method.GET;

/**
 * REST action to retrieve search pipelines
 *
 *  @density.internal
 */
public class RestGetSearchPipelineAction extends BaseRestHandler {
    @Override
    public String getName() {
        return "search_get_pipeline_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_search/pipeline"), new Route(GET, "/_search/pipeline/{id}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        GetSearchPipelineRequest request = new GetSearchPipelineRequest(Strings.splitStringByCommaToArray(restRequest.param("id")));
        request.clusterManagerNodeTimeout(restRequest.paramAsTime("cluster_manager_timeout", request.clusterManagerNodeTimeout()));
        return channel -> client.admin().cluster().getSearchPipeline(request, new RestStatusToXContentListener<>(channel));
    }
}
