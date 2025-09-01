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

package org.density.rest.action.admin.indices;

import org.density.action.admin.indices.template.get.GetIndexTemplatesRequest;
import org.density.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.density.common.logging.DeprecationLogger;
import org.density.common.settings.Settings;
import org.density.core.common.Strings;
import org.density.core.rest.RestStatus;
import org.density.rest.BaseRestHandler;
import org.density.rest.RestRequest;
import org.density.rest.action.RestToXContentListener;
import org.density.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.density.core.rest.RestStatus.NOT_FOUND;
import static org.density.core.rest.RestStatus.OK;
import static org.density.rest.RestRequest.Method.GET;
import static org.density.rest.RestRequest.Method.HEAD;

/**
 * The REST handler for get template and head template APIs.
 *
 * @density.api
 */
public class RestGetIndexTemplateAction extends BaseRestHandler {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestGetIndexTemplateAction.class);

    @Override
    public List<Route> routes() {
        return unmodifiableList(
            asList(new Route(GET, "/_template"), new Route(GET, "/_template/{name}"), new Route(HEAD, "/_template/{name}"))
        );
    }

    @Override
    public String getName() {
        return "get_index_template_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final String[] names = Strings.splitStringByCommaToArray(request.param("name"));

        final GetIndexTemplatesRequest getIndexTemplatesRequest = new GetIndexTemplatesRequest(names);
        getIndexTemplatesRequest.local(request.paramAsBoolean("local", getIndexTemplatesRequest.local()));
        getIndexTemplatesRequest.clusterManagerNodeTimeout(
            request.paramAsTime("cluster_manager_timeout", getIndexTemplatesRequest.clusterManagerNodeTimeout())
        );
        parseDeprecatedMasterTimeoutParameter(getIndexTemplatesRequest, request, deprecationLogger, getName());

        final boolean implicitAll = getIndexTemplatesRequest.names().length == 0;

        return channel -> client.admin()
            .indices()
            .getTemplates(getIndexTemplatesRequest, new RestToXContentListener<GetIndexTemplatesResponse>(channel) {
                @Override
                protected RestStatus getStatus(final GetIndexTemplatesResponse response) {
                    final boolean templateExists = response.getIndexTemplates().isEmpty() == false;
                    return (templateExists || implicitAll) ? OK : NOT_FOUND;
                }
            });
    }

    @Override
    protected Set<String> responseParams() {
        return Settings.FORMAT_PARAMS;
    }

}
