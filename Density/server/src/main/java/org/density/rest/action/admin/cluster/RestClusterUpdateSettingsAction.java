/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright Density Contributors. See
 * GitHub history for details.
 */

package org.density.rest.action.admin.cluster;

import org.density.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.density.common.logging.DeprecationLogger;
import org.density.common.settings.Settings;
import org.density.core.xcontent.XContentParser;
import org.density.rest.BaseRestHandler;
import org.density.rest.RestRequest;
import org.density.rest.action.RestToXContentListener;
import org.density.transport.client.Requests;
import org.density.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.singletonList;
import static org.density.rest.RestRequest.Method.PUT;

/**
 * Transport action to update cluster settings
 *
 * @density.api
 */
public class RestClusterUpdateSettingsAction extends BaseRestHandler {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestClusterUpdateSettingsAction.class);

    private static final String PERSISTENT = "persistent";
    private static final String TRANSIENT = "transient";

    @Override
    public List<Route> routes() {
        return singletonList(new Route(PUT, "/_cluster/settings"));
    }

    @Override
    public String getName() {
        return "cluster_update_settings_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final ClusterUpdateSettingsRequest clusterUpdateSettingsRequest = Requests.clusterUpdateSettingsRequest();
        clusterUpdateSettingsRequest.timeout(request.paramAsTime("timeout", clusterUpdateSettingsRequest.timeout()));
        clusterUpdateSettingsRequest.clusterManagerNodeTimeout(
            request.paramAsTime("cluster_manager_timeout", clusterUpdateSettingsRequest.clusterManagerNodeTimeout())
        );
        parseDeprecatedMasterTimeoutParameter(clusterUpdateSettingsRequest, request, deprecationLogger, getName());
        Map<String, Object> source;
        try (XContentParser parser = request.contentParser()) {
            source = parser.map();
        }
        if (source.containsKey(TRANSIENT)) {
            clusterUpdateSettingsRequest.transientSettings((Map) source.get(TRANSIENT));
        }
        if (source.containsKey(PERSISTENT)) {
            clusterUpdateSettingsRequest.persistentSettings((Map) source.get(PERSISTENT));
        }

        return channel -> client.admin().cluster().updateSettings(clusterUpdateSettingsRequest, new RestToXContentListener<>(channel));
    }

    @Override
    protected Set<String> responseParams() {
        return Settings.FORMAT_PARAMS;
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }
}
