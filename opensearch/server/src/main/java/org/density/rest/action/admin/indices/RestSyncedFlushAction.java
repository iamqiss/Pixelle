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

import org.density.action.admin.indices.flush.FlushRequest;
import org.density.action.admin.indices.flush.FlushResponse;
import org.density.action.support.IndicesOptions;
import org.density.common.logging.DeprecationLogger;
import org.density.core.common.Strings;
import org.density.core.rest.RestStatus;
import org.density.core.xcontent.XContentBuilder;
import org.density.rest.BaseRestHandler;
import org.density.rest.BytesRestResponse;
import org.density.rest.RestChannel;
import org.density.rest.RestRequest;
import org.density.rest.RestResponse;
import org.density.rest.action.RestToXContentListener;
import org.density.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.density.rest.RestRequest.Method.GET;
import static org.density.rest.RestRequest.Method.POST;

/**
 * Transport action to execute a synced flush
 *
 * @density.api
 *
 * @deprecated remove since synced flush is removed
 */
@Deprecated
public class RestSyncedFlushAction extends BaseRestHandler {

    private static final DeprecationLogger DEPRECATION_LOGGER = DeprecationLogger.getLogger(RestSyncedFlushAction.class);

    @Override
    public List<Route> routes() {
        return unmodifiableList(
            asList(
                new Route(GET, "/_flush/synced"),
                new Route(POST, "/_flush/synced"),
                new Route(GET, "/{index}/_flush/synced"),
                new Route(POST, "/{index}/_flush/synced")
            )
        );
    }

    @Override
    public String getName() {
        return "synced_flush_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        DEPRECATION_LOGGER.deprecate(
            "synced_flush",
            "Synced flush was removed and a normal flush was performed instead. This transition will be removed in a future version."
        );
        final FlushRequest flushRequest = new FlushRequest(Strings.splitStringByCommaToArray(request.param("index")));
        flushRequest.indicesOptions(IndicesOptions.fromRequest(request, flushRequest.indicesOptions()));
        return channel -> client.admin().indices().flush(flushRequest, new SimulateSyncedFlushResponseListener(channel));
    }

    static final class SimulateSyncedFlushResponseListener extends RestToXContentListener<FlushResponse> {

        SimulateSyncedFlushResponseListener(RestChannel channel) {
            super(channel);
        }

        @Override
        public RestResponse buildResponse(FlushResponse flushResponse, XContentBuilder builder) throws Exception {
            builder.startObject();
            buildSyncedFlushResponse(builder, flushResponse);
            builder.endObject();
            final RestStatus restStatus = flushResponse.getFailedShards() == 0 ? RestStatus.OK : RestStatus.CONFLICT;
            return new BytesRestResponse(restStatus, builder);
        }

        private void buildSyncedFlushResponse(XContentBuilder builder, FlushResponse flushResponse) throws IOException {
            builder.startObject("_shards");
            builder.field("total", flushResponse.getTotalShards());
            builder.field("successful", flushResponse.getSuccessfulShards());
            builder.field("failed", flushResponse.getFailedShards());
            // can't serialize the detail of each index as we don't have the shard count per index.
            builder.endObject();
        }
    }
}
