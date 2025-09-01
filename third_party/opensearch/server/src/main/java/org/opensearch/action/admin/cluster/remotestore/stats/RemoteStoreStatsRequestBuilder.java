/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.cluster.remotestore.stats;

import org.density.action.support.broadcast.BroadcastOperationRequestBuilder;
import org.density.common.annotation.PublicApi;
import org.density.common.unit.TimeValue;
import org.density.transport.client.DensityClient;

/**
 * Builder for RemoteStoreStatsRequest
 *
 * @density.api
 */
@PublicApi(since = "2.8.0")
public class RemoteStoreStatsRequestBuilder extends BroadcastOperationRequestBuilder<
    RemoteStoreStatsRequest,
    RemoteStoreStatsResponse,
    RemoteStoreStatsRequestBuilder> {

    public RemoteStoreStatsRequestBuilder(DensityClient client, RemoteStoreStatsAction action) {
        super(client, action, new RemoteStoreStatsRequest());
    }

    /**
     * Sets timeout of request.
     */
    public final RemoteStoreStatsRequestBuilder setTimeout(TimeValue timeout) {
        request.timeout(timeout);
        return this;
    }

    /**
     * Sets shards preference of request.
     */
    public final RemoteStoreStatsRequestBuilder setShards(String... shards) {
        request.shards(shards);
        return this;
    }

    /**
     * Sets local shards preference of request.
     */
    public final RemoteStoreStatsRequestBuilder setLocal(boolean local) {
        request.local(local);
        return this;
    }
}
