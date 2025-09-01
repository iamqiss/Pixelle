/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.rest.action.admin.indices;

import org.density.action.admin.indices.streamingingestion.state.GetIngestionStateRequest;
import org.density.action.pagination.PageParams;
import org.density.action.support.IndicesOptions;
import org.density.common.annotation.ExperimentalApi;
import org.density.core.common.Strings;
import org.density.rest.BaseRestHandler;
import org.density.rest.RestRequest;
import org.density.rest.action.RestToXContentListener;
import org.density.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.density.action.admin.indices.streamingingestion.state.GetIngestionStateRequest.DEFAULT_PAGE_SIZE;
import static org.density.action.admin.indices.streamingingestion.state.GetIngestionStateRequest.DEFAULT_SORT_VALUE;
import static org.density.rest.RestRequest.Method.GET;

/**
 * Transport action to get ingestion state. This API supports pagination.
 *
 * @density.experimental
 */
@ExperimentalApi
public class RestGetIngestionStateAction extends BaseRestHandler {
    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(new Route(GET, "/{index}/ingestion/_state")));
    }

    @Override
    public String getName() {
        return "get_ingestion_state_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        GetIngestionStateRequest getIngestionStateRequest = new GetIngestionStateRequest(
            Strings.splitStringByCommaToArray(request.param("index"))
        );

        if (request.hasParam("shards")) {
            int[] shards = Arrays.stream(request.paramAsStringArrayOrEmptyIfAll("shards"))
                .mapToInt(Integer::parseInt) // Convert each string to int
                .toArray();
            getIngestionStateRequest.setShards(shards);
        }
        getIngestionStateRequest.timeout(request.paramAsTime("timeout", getIngestionStateRequest.timeout()));
        getIngestionStateRequest.indicesOptions(IndicesOptions.fromRequest(request, getIngestionStateRequest.indicesOptions()));

        PageParams pageParams = validateAndGetPageParams(request, DEFAULT_SORT_VALUE, DEFAULT_PAGE_SIZE);
        getIngestionStateRequest.setPageParams(pageParams);
        return channel -> client.admin().indices().getIngestionState(getIngestionStateRequest, new RestToXContentListener<>(channel));
    }

    @Override
    public boolean isActionPaginated() {
        return true;
    }
}
