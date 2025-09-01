/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.rest.action.search;

import org.density.action.search.PutSearchPipelineRequest;
import org.density.common.collect.Tuple;
import org.density.core.common.bytes.BytesReference;
import org.density.core.xcontent.MediaType;
import org.density.rest.BaseRestHandler;
import org.density.rest.RestRequest;
import org.density.rest.action.RestToXContentListener;
import org.density.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static org.density.rest.RestRequest.Method.PUT;

/**
 * REST action to put a search pipeline
 *
 * @density.internal
 */
public class RestPutSearchPipelineAction extends BaseRestHandler {
    @Override
    public String getName() {
        return "search_put_pipeline_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(PUT, "/_search/pipeline/{id}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        Tuple<MediaType, BytesReference> sourceTuple = restRequest.contentOrSourceParam();
        PutSearchPipelineRequest request = new PutSearchPipelineRequest(restRequest.param("id"), sourceTuple.v2(), sourceTuple.v1());
        request.clusterManagerNodeTimeout(restRequest.paramAsTime("cluster_manager_timeout", request.clusterManagerNodeTimeout()));
        request.timeout(restRequest.paramAsTime("timeout", request.timeout()));
        return channel -> client.admin().cluster().putSearchPipeline(request, new RestToXContentListener<>(channel));
    }
}
