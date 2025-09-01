/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.plugin.wlm.rest;

import org.density.core.rest.RestStatus;
import org.density.core.xcontent.ToXContent;
import org.density.plugin.wlm.action.GetWorkloadGroupAction;
import org.density.plugin.wlm.action.GetWorkloadGroupRequest;
import org.density.plugin.wlm.action.GetWorkloadGroupResponse;
import org.density.rest.BaseRestHandler;
import org.density.rest.BytesRestResponse;
import org.density.rest.RestChannel;
import org.density.rest.RestRequest;
import org.density.rest.RestResponse;
import org.density.rest.action.RestResponseListener;
import org.density.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static org.density.rest.RestRequest.Method.GET;

/**
 * Rest action to get a WorkloadGroup
 *
 * @density.experimental
 */
public class RestGetWorkloadGroupAction extends BaseRestHandler {

    /**
     * Constructor for RestGetWorkloadGroupAction
     */
    public RestGetWorkloadGroupAction() {}

    @Override
    public String getName() {
        return "get_workload_group";
    }

    /**
     * The list of {@link Route}s that this RestHandler is responsible for handling.
     */
    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "_wlm/workload_group/{name}"), new Route(GET, "_wlm/workload_group/"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final GetWorkloadGroupRequest getWorkloadGroupRequest = new GetWorkloadGroupRequest(request.param("name"));
        return channel -> client.execute(GetWorkloadGroupAction.INSTANCE, getWorkloadGroupRequest, getWorkloadGroupResponse(channel));
    }

    private RestResponseListener<GetWorkloadGroupResponse> getWorkloadGroupResponse(final RestChannel channel) {
        return new RestResponseListener<>(channel) {
            @Override
            public RestResponse buildResponse(final GetWorkloadGroupResponse response) throws Exception {
                return new BytesRestResponse(RestStatus.OK, response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS));
            }
        };
    }
}
