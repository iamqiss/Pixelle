/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.rest.action.admin.cluster;

import org.density.action.admin.cluster.remotestore.restore.RestoreRemoteStoreRequest;
import org.density.rest.BaseRestHandler;
import org.density.rest.RestRequest;
import org.density.rest.action.RestToXContentListener;
import org.density.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.density.rest.RestRequest.Method.POST;

/**
 * Restores data from remote store
 *
 * @density.api
 */
public final class RestRestoreRemoteStoreAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return singletonList(new Route(POST, "/_remotestore/_restore"));
    }

    @Override
    public String getName() {
        return "restore_remote_store_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        RestoreRemoteStoreRequest restoreRemoteStoreRequest = new RestoreRemoteStoreRequest();
        restoreRemoteStoreRequest.clusterManagerNodeTimeout(
            request.paramAsTime("cluster_manager_timeout", restoreRemoteStoreRequest.clusterManagerNodeTimeout())
        );
        restoreRemoteStoreRequest.waitForCompletion(request.paramAsBoolean("wait_for_completion", false));
        restoreRemoteStoreRequest.restoreAllShards(request.paramAsBoolean("restore_all_shards", false));
        request.applyContentParser(p -> restoreRemoteStoreRequest.source(p.mapOrdered()));
        return channel -> client.admin().cluster().restoreRemoteStore(restoreRemoteStoreRequest, new RestToXContentListener<>(channel));
    }
}
