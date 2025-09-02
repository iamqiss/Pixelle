/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.plugin.wlm.rest;

import org.density.plugin.wlm.WlmClusterSettingValuesProvider;
import org.density.plugin.wlm.action.DeleteWorkloadGroupAction;
import org.density.plugin.wlm.action.DeleteWorkloadGroupRequest;
import org.density.rest.BaseRestHandler;
import org.density.rest.RestRequest;
import org.density.rest.action.RestToXContentListener;
import org.density.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static org.density.rest.RestRequest.Method.DELETE;

/**
 * Rest action to delete a WorkloadGroup
 *
 * @density.experimental
 */
public class RestDeleteWorkloadGroupAction extends BaseRestHandler {

    private final WlmClusterSettingValuesProvider nonPluginSettingValuesProvider;

    /**
     * Constructor for RestDeleteWorkloadGroupAction
     * @param nonPluginSettingValuesProvider the settings provider to access the current WLM mode
     */
    public RestDeleteWorkloadGroupAction(WlmClusterSettingValuesProvider nonPluginSettingValuesProvider) {
        this.nonPluginSettingValuesProvider = nonPluginSettingValuesProvider;
    }

    @Override
    public String getName() {
        return "delete_workload_group";
    }

    /**
     * The list of {@link Route}s that this RestHandler is responsible for handling.
     */
    @Override
    public List<Route> routes() {
        return List.of(new Route(DELETE, "_wlm/workload_group/{name}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        nonPluginSettingValuesProvider.ensureWlmEnabled(getName());
        DeleteWorkloadGroupRequest deleteWorkloadGroupRequest = new DeleteWorkloadGroupRequest(request.param("name"));
        deleteWorkloadGroupRequest.clusterManagerNodeTimeout(
            request.paramAsTime("cluster_manager_timeout", deleteWorkloadGroupRequest.clusterManagerNodeTimeout())
        );
        deleteWorkloadGroupRequest.timeout(request.paramAsTime("timeout", deleteWorkloadGroupRequest.timeout()));
        return channel -> client.execute(
            DeleteWorkloadGroupAction.INSTANCE,
            deleteWorkloadGroupRequest,
            new RestToXContentListener<>(channel)
        );
    }
}
