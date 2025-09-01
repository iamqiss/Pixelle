/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.indices.tiering;

import org.density.action.support.IndicesOptions;
import org.density.common.annotation.ExperimentalApi;
import org.density.rest.BaseRestHandler;
import org.density.rest.RestHandler;
import org.density.rest.RestRequest;
import org.density.rest.action.RestToXContentListener;
import org.density.transport.client.node.NodeClient;

import java.util.List;

import static java.util.Collections.singletonList;
import static org.density.core.common.Strings.splitStringByCommaToArray;
import static org.density.rest.RestRequest.Method.POST;

/**
 * Rest Tiering API action to move indices to warm tier
 *
 * @density.experimental
 */
@ExperimentalApi
public class RestWarmTieringAction extends BaseRestHandler {

    private static final String TARGET_TIER = "warm";

    @Override
    public List<RestHandler.Route> routes() {
        return singletonList(new RestHandler.Route(POST, "/{index}/_tier/" + TARGET_TIER));
    }

    @Override
    public String getName() {
        return "warm_tiering_action";
    }

    @Override
    protected BaseRestHandler.RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        final TieringIndexRequest tieringIndexRequest = new TieringIndexRequest(
            TARGET_TIER,
            splitStringByCommaToArray(request.param("index"))
        );
        tieringIndexRequest.timeout(request.paramAsTime("timeout", tieringIndexRequest.timeout()));
        tieringIndexRequest.clusterManagerNodeTimeout(
            request.paramAsTime("cluster_manager_timeout", tieringIndexRequest.clusterManagerNodeTimeout())
        );
        tieringIndexRequest.indicesOptions(IndicesOptions.fromRequest(request, tieringIndexRequest.indicesOptions()));
        tieringIndexRequest.waitForCompletion(request.paramAsBoolean("wait_for_completion", tieringIndexRequest.waitForCompletion()));
        return channel -> client.admin()
            .cluster()
            .execute(HotToWarmTieringAction.INSTANCE, tieringIndexRequest, new RestToXContentListener<>(channel));
    }
}
