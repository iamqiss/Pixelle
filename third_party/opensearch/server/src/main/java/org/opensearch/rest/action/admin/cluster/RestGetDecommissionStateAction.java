/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.rest.action.admin.cluster;

import org.density.action.admin.cluster.decommission.awareness.get.GetDecommissionStateRequest;
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
 * Returns decommissioned attribute information
 *
 * @density.api
 */
public class RestGetDecommissionStateAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return singletonList(new Route(GET, "/_cluster/decommission/awareness/{awareness_attribute_name}/_status"));
    }

    @Override
    public String getName() {
        return "get_decommission_state_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        GetDecommissionStateRequest getDecommissionStateRequest = Requests.getDecommissionStateRequest();
        String attributeName = request.param("awareness_attribute_name");
        getDecommissionStateRequest.attributeName(attributeName);
        return channel -> client.admin().cluster().getDecommissionState(getDecommissionStateRequest, new RestToXContentListener<>(channel));
    }
}
