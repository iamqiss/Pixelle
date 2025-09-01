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
import org.density.action.admin.cluster.shards.routing.weighted.put.ClusterPutWeightedRoutingRequest;
import org.density.rest.BaseRestHandler;
import org.density.rest.RestRequest;
import org.density.rest.action.RestToXContentListener;
import org.density.transport.client.Requests;
import org.density.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.density.rest.RestRequest.Method.PUT;

/**
 * Update Weighted Round Robin based shard routing weights
 *
 * @density.api
 *
 */
public class RestClusterPutWeightedRoutingAction extends BaseRestHandler {

    private static final Logger logger = LogManager.getLogger(RestClusterPutWeightedRoutingAction.class);

    @Override
    public List<Route> routes() {
        return singletonList(new Route(PUT, "/_cluster/routing/awareness/{attribute}/weights"));
    }

    @Override
    public String getName() {
        return "put_weighted_routing_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        ClusterPutWeightedRoutingRequest putWeightedRoutingRequest = createRequest(request);
        return channel -> client.admin().cluster().putWeightedRouting(putWeightedRoutingRequest, new RestToXContentListener<>(channel));
    }

    public static ClusterPutWeightedRoutingRequest createRequest(RestRequest request) throws IOException {
        ClusterPutWeightedRoutingRequest putWeightedRoutingRequest = Requests.putWeightedRoutingRequest(request.param("attribute"));
        request.applyContentParser(p -> putWeightedRoutingRequest.source(p.mapOrdered()));
        return putWeightedRoutingRequest;
    }

}
