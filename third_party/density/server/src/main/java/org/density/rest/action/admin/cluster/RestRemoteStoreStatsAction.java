/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.rest.action.admin.cluster;

import org.density.action.admin.cluster.remotestore.stats.RemoteStoreStatsRequest;
import org.density.rest.BaseRestHandler;
import org.density.rest.RestRequest;
import org.density.rest.action.RestToXContentListener;
import org.density.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.density.rest.RestRequest.Method.GET;

/**
 * Rest action for fetching remote store stats
 *
 * @density.internal
 */
public class RestRemoteStoreStatsAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return unmodifiableList(
            asList(new Route(GET, "/_remotestore/stats/{index}"), new Route(GET, "/_remotestore/stats/{index}/{shard_id}"))
        );
    }

    @Override
    public String getName() {
        return "remote_store_stats";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String index = request.param("index");
        String shardId = request.param("shard_id");
        boolean local = Objects.equals(request.param("local"), "true");
        RemoteStoreStatsRequest remoteStoreStatsRequest = new RemoteStoreStatsRequest();
        if (index != null) {
            remoteStoreStatsRequest.indices(index);
        }
        if (shardId != null) {
            remoteStoreStatsRequest.shards(shardId);
        }
        remoteStoreStatsRequest.local(local);
        return channel -> client.admin().cluster().remoteStoreStats(remoteStoreStatsRequest, new RestToXContentListener<>(channel));
    }
}
