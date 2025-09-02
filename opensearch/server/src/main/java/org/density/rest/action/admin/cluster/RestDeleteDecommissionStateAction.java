/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.rest.action.admin.cluster;

import org.density.action.admin.cluster.decommission.awareness.delete.DeleteDecommissionStateRequest;
import org.density.rest.BaseRestHandler;
import org.density.rest.RestRequest;
import org.density.rest.action.RestToXContentListener;
import org.density.transport.client.Requests;
import org.density.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.density.rest.RestRequest.Method.DELETE;

/**
 * Clears the decommission metadata.
 *
 * @density.api
 */
public class RestDeleteDecommissionStateAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return singletonList(new Route(DELETE, "/_cluster/decommission/awareness"));
    }

    @Override
    public String getName() {
        return "delete_decommission_state_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        DeleteDecommissionStateRequest deleteDecommissionStateRequest = createRequest();
        return channel -> client.admin()
            .cluster()
            .deleteDecommissionState(deleteDecommissionStateRequest, new RestToXContentListener<>(channel));
    }

    DeleteDecommissionStateRequest createRequest() {
        return Requests.deleteDecommissionStateRequest();
    }
}
