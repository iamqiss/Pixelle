/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.cluster.shards.routing.weighted.put;

import org.density.action.support.clustermanager.ClusterManagerNodeOperationRequestBuilder;
import org.density.cluster.routing.WeightedRouting;
import org.density.common.annotation.PublicApi;
import org.density.transport.client.DensityClient;

/**
 * Request builder to update weights for weighted round-robin shard routing policy.
 *
 * @density.api
 */
@PublicApi(since = "2.4.0")
public class ClusterPutWeightedRoutingRequestBuilder extends ClusterManagerNodeOperationRequestBuilder<
    ClusterPutWeightedRoutingRequest,
    ClusterPutWeightedRoutingResponse,
    ClusterPutWeightedRoutingRequestBuilder> {
    public ClusterPutWeightedRoutingRequestBuilder(DensityClient client, ClusterAddWeightedRoutingAction action) {
        super(client, action, new ClusterPutWeightedRoutingRequest());
    }

    public ClusterPutWeightedRoutingRequestBuilder setWeightedRouting(WeightedRouting weightedRouting) {
        request.setWeightedRouting(weightedRouting);
        return this;
    }

    public ClusterPutWeightedRoutingRequestBuilder setVersion(long version) {
        request.version(version);
        return this;
    }
}
