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

package org.density.rest.action.search;

import org.density.action.search.SearchRequest;
import org.density.action.search.SearchResponse;
import org.density.action.support.IndicesOptions;
import org.density.core.common.Strings;
import org.density.core.xcontent.XContentBuilder;
import org.density.index.query.QueryBuilder;
import org.density.rest.BaseRestHandler;
import org.density.rest.BytesRestResponse;
import org.density.rest.RestRequest;
import org.density.rest.RestResponse;
import org.density.rest.action.RestActions;
import org.density.rest.action.RestBuilderListener;
import org.density.search.builder.SearchSourceBuilder;
import org.density.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.density.rest.RestRequest.Method.GET;
import static org.density.rest.RestRequest.Method.POST;
import static org.density.rest.action.RestActions.buildBroadcastShardsHeader;
import static org.density.search.internal.SearchContext.DEFAULT_TERMINATE_AFTER;

/**
 * Transport action to count documents
 *
 * @density.api
 */
public class RestCountAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return unmodifiableList(
            asList(
                new Route(GET, "/_count"),
                new Route(POST, "/_count"),
                new Route(GET, "/{index}/_count"),
                new Route(POST, "/{index}/_count")
            )
        );
    }

    @Override
    public String getName() {
        return "count_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        SearchRequest countRequest = new SearchRequest(Strings.splitStringByCommaToArray(request.param("index")));
        countRequest.indicesOptions(IndicesOptions.fromRequest(request, countRequest.indicesOptions()));
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().size(0).trackTotalHits(true);
        countRequest.source(searchSourceBuilder);
        request.withContentOrSourceParamParserOrNull(parser -> {
            if (parser == null) {
                QueryBuilder queryBuilder = RestActions.urlParamsToQueryBuilder(request);
                if (queryBuilder != null) {
                    searchSourceBuilder.query(queryBuilder);
                }
            } else {
                searchSourceBuilder.query(RestActions.getQueryContent(parser));
            }
        });
        countRequest.routing(request.param("routing"));
        float minScore = request.paramAsFloat("min_score", -1f);
        if (minScore != -1f) {
            searchSourceBuilder.minScore(minScore);
        }

        countRequest.preference(request.param("preference"));

        final int terminateAfter = request.paramAsInt("terminate_after", DEFAULT_TERMINATE_AFTER);
        if (terminateAfter < 0) {
            throw new IllegalArgumentException("terminateAfter must be > 0");
        } else if (terminateAfter > 0) {
            searchSourceBuilder.terminateAfter(terminateAfter);
        }
        return channel -> client.search(countRequest, new RestBuilderListener<SearchResponse>(channel) {
            @Override
            public RestResponse buildResponse(SearchResponse response, XContentBuilder builder) throws Exception {
                builder.startObject();
                if (terminateAfter != DEFAULT_TERMINATE_AFTER) {
                    builder.field("terminated_early", response.isTerminatedEarly());
                }
                builder.field("count", response.getHits().getTotalHits().value());
                buildBroadcastShardsHeader(
                    builder,
                    request,
                    response.getTotalShards(),
                    response.getSuccessfulShards(),
                    0,
                    response.getFailedShards(),
                    response.getShardFailures()
                );

                builder.endObject();
                return new BytesRestResponse(response.status(), builder);
            }
        });
    }

}
