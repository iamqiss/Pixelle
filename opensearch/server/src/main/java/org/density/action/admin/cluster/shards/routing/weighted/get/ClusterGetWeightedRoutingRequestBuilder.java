/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.cluster.shards.routing.weighted.get;

import org.density.action.support.clustermanager.ClusterManagerNodeReadOperationRequestBuilder;
import org.density.common.annotation.PublicApi;
import org.density.transport.client.DensityClient;

/**
 * Request builder to get weights for weighted round-robin search routing policy.
 *
 * @density.api
 */
@PublicApi(since = "2.4.0")
public class ClusterGetWeightedRoutingRequestBuilder extends ClusterManagerNodeReadOperationRequestBuilder<
    ClusterGetWeightedRoutingRequest,
    ClusterGetWeightedRoutingResponse,
    ClusterGetWeightedRoutingRequestBuilder> {

    public ClusterGetWeightedRoutingRequestBuilder(DensityClient client, ClusterGetWeightedRoutingAction action) {
        super(client, action, new ClusterGetWeightedRoutingRequest());
    }

    public ClusterGetWeightedRoutingRequestBuilder setRequestLocal(boolean local) {
        request.local(local);
        return this;
    }

    public ClusterGetWeightedRoutingRequestBuilder setAwarenessAttribute(String attribute) {
        request.setAwarenessAttribute(attribute);
        return this;
    }
}
