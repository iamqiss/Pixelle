/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.cluster.shards.routing.weighted.delete;

import org.density.action.support.clustermanager.ClusterManagerNodeOperationRequestBuilder;
import org.density.common.annotation.PublicApi;
import org.density.transport.client.DensityClient;

/**
 * Request builder to delete weights for weighted round-robin shard routing policy.
 *
 * @density.api
 */
@PublicApi(since = "2.4.0")
public class ClusterDeleteWeightedRoutingRequestBuilder extends ClusterManagerNodeOperationRequestBuilder<
    ClusterDeleteWeightedRoutingRequest,
    ClusterDeleteWeightedRoutingResponse,
    ClusterDeleteWeightedRoutingRequestBuilder> {

    public ClusterDeleteWeightedRoutingRequestBuilder(DensityClient client, ClusterDeleteWeightedRoutingAction action) {
        super(client, action, new ClusterDeleteWeightedRoutingRequest());
    }

    public ClusterDeleteWeightedRoutingRequestBuilder setVersion(long version) {
        request.setVersion(version);
        return this;
    }

    public ClusterDeleteWeightedRoutingRequestBuilder setAwarenessAttribute(String attribute) {
        request.setAwarenessAttribute(attribute);
        return this;
    }

}
