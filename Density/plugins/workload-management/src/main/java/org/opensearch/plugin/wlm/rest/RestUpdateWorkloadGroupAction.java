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
import org.density.core.xcontent.XContentParser;
import org.density.plugin.wlm.WlmClusterSettingValuesProvider;
import org.density.plugin.wlm.action.UpdateWorkloadGroupAction;
import org.density.plugin.wlm.action.UpdateWorkloadGroupRequest;
import org.density.plugin.wlm.action.UpdateWorkloadGroupResponse;
import org.density.rest.BaseRestHandler;
import org.density.rest.BytesRestResponse;
import org.density.rest.RestChannel;
import org.density.rest.RestRequest;
import org.density.rest.RestResponse;
import org.density.rest.action.RestResponseListener;
import org.density.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static org.density.rest.RestRequest.Method.POST;
import static org.density.rest.RestRequest.Method.PUT;

/**
 * Rest action to update a WorkloadGroup
 *
 * @density.experimental
 */
public class RestUpdateWorkloadGroupAction extends BaseRestHandler {

    private final WlmClusterSettingValuesProvider nonPluginSettingValuesProvider;

    /**
     * Constructor for RestUpdateWorkloadGroupAction
     * @param nonPluginSettingValuesProvider the settings provider to access the current WLM mode
     */
    public RestUpdateWorkloadGroupAction(WlmClusterSettingValuesProvider nonPluginSettingValuesProvider) {
        this.nonPluginSettingValuesProvider = nonPluginSettingValuesProvider;
    }

    @Override
    public String getName() {
        return "update_workload_group";
    }

    /**
     * The list of {@link Route}s that this RestHandler is responsible for handling.
     */
    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "_wlm/workload_group/{name}"), new Route(PUT, "_wlm/workload_group/{name}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        nonPluginSettingValuesProvider.ensureWlmEnabled(getName());
        try (XContentParser parser = request.contentParser()) {
            UpdateWorkloadGroupRequest updateWorkloadGroupRequest = UpdateWorkloadGroupRequest.fromXContent(parser, request.param("name"));
            return channel -> client.execute(
                UpdateWorkloadGroupAction.INSTANCE,
                updateWorkloadGroupRequest,
                updateWorkloadGroupResponse(channel)
            );
        }
    }

    private RestResponseListener<UpdateWorkloadGroupResponse> updateWorkloadGroupResponse(final RestChannel channel) {
        return new RestResponseListener<>(channel) {
            @Override
            public RestResponse buildResponse(final UpdateWorkloadGroupResponse response) throws Exception {
                return new BytesRestResponse(RestStatus.OK, response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS));
            }
        };
    }
}
