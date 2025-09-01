/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.cluster.decommission.awareness.get;

import org.density.action.support.clustermanager.ClusterManagerNodeReadOperationRequestBuilder;
import org.density.common.annotation.PublicApi;
import org.density.transport.client.DensityClient;

/**
 * Get decommission request builder
 *
 * @density.api
 */
@PublicApi(since = "2.4.0")
public class GetDecommissionStateRequestBuilder extends ClusterManagerNodeReadOperationRequestBuilder<
    GetDecommissionStateRequest,
    GetDecommissionStateResponse,
    GetDecommissionStateRequestBuilder> {

    /**
     * Creates new get decommissioned attributes request builder
     */
    public GetDecommissionStateRequestBuilder(DensityClient client, GetDecommissionStateAction action) {
        super(client, action, new GetDecommissionStateRequest());
    }

    /**
     * @param attributeName name of attribute
     * @return current object
     */
    public GetDecommissionStateRequestBuilder setAttributeName(String attributeName) {
        request.attributeName(attributeName);
        return this;
    }
}
