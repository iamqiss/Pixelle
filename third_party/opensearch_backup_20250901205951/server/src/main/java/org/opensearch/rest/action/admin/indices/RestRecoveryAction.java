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

import org.density.action.admin.indices.recovery.RecoveryRequest;
import org.density.action.support.IndicesOptions;
import org.density.core.common.Strings;
import org.density.rest.BaseRestHandler;
import org.density.rest.RestRequest;
import org.density.rest.action.RestToXContentListener;
import org.density.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.density.rest.RestRequest.Method.GET;

/**
 * REST handler to report on index recoveries.
 *
 * @density.api
 */
public class RestRecoveryAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(new Route(GET, "/_recovery"), new Route(GET, "/{index}/_recovery")));
    }

    @Override
    public String getName() {
        return "recovery_action";
    }

    @Override
    public boolean allowSystemIndexAccessByDefault() {
        return true;
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {

        final RecoveryRequest recoveryRequest = new RecoveryRequest(Strings.splitStringByCommaToArray(request.param("index")));
        recoveryRequest.detailed(request.paramAsBoolean("detailed", false));
        recoveryRequest.activeOnly(request.paramAsBoolean("active_only", false));
        recoveryRequest.indicesOptions(IndicesOptions.fromRequest(request, recoveryRequest.indicesOptions()));
        return channel -> client.admin().indices().recoveries(recoveryRequest, new RestToXContentListener<>(channel));
    }
}
