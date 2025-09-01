/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.cluster.remotestore.metadata;

import org.density.action.support.broadcast.BroadcastOperationRequestBuilder;
import org.density.common.annotation.ExperimentalApi;
import org.density.common.unit.TimeValue;
import org.density.transport.client.DensityClient;

/**
 * Builder for RemoteStoreMetadataRequest
 *
 * @density.api
 */
@ExperimentalApi
public class RemoteStoreMetadataRequestBuilder extends BroadcastOperationRequestBuilder<
    RemoteStoreMetadataRequest,
    RemoteStoreMetadataResponse,
    RemoteStoreMetadataRequestBuilder> {

    public RemoteStoreMetadataRequestBuilder(DensityClient client, RemoteStoreMetadataAction action) {
        super(client, action, new RemoteStoreMetadataRequest());
    }

    /**
     * Sets timeout of request.
     */
    public final RemoteStoreMetadataRequestBuilder setTimeout(TimeValue timeout) {
        request.timeout(timeout);
        return this;
    }

    /**
     * Sets shards preference of request.
     */
    public final RemoteStoreMetadataRequestBuilder setShards(String... shards) {
        request.shards(shards);
        return this;
    }
}
