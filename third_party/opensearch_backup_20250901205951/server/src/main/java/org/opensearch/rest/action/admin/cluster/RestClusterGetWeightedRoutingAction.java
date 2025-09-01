/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.rest.action.admin.cluster;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.density.action.admin.cluster.shards.routing.weighted.get.ClusterGetWeightedRoutingRequest;
import org.density.rest.BaseRestHandler;
import org.density.rest.RestRequest;
import org.density.rest.action.RestToXContentListener;
import org.density.transport.client.Requests;
import org.density.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.density.rest.RestRequest.Method.GET;

/**
 * Fetch Weighted Round Robin based shard routing weights
 *
 * @density.api
 *
 */
public class RestClusterGetWeightedRoutingAction extends BaseRestHandler {

    private static final Logger logger = LogManager.getLogger(RestClusterGetWeightedRoutingAction.class);

    @Override
    public List<Route> routes() {
        return singletonList(new Route(GET, "/_cluster/routing/awareness/{attribute}/weights"));
    }

    @Override
    public String getName() {
        return "get_weighted_routing_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        ClusterGetWeightedRoutingRequest getWeightedRoutingRequest = Requests.getWeightedRoutingRequest(request.param("attribute"));
        getWeightedRoutingRequest.local(request.paramAsBoolean("local", getWeightedRoutingRequest.local()));
        return channel -> client.admin().cluster().getWeightedRouting(getWeightedRoutingRequest, new RestToXContentListener<>(channel));
    }
}
