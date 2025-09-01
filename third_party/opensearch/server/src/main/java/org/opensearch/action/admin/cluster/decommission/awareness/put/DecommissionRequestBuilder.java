/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.cluster.decommission.awareness.put;

import org.density.action.ActionType;
import org.density.action.support.clustermanager.ClusterManagerNodeOperationRequestBuilder;
import org.density.cluster.decommission.DecommissionAttribute;
import org.density.common.annotation.PublicApi;
import org.density.common.unit.TimeValue;
import org.density.transport.client.DensityClient;

/**
 * Register decommission request builder
 *
 * @density.api
 */
@PublicApi(since = "2.4.0")
public class DecommissionRequestBuilder extends ClusterManagerNodeOperationRequestBuilder<
    DecommissionRequest,
    DecommissionResponse,
    DecommissionRequestBuilder> {

    public DecommissionRequestBuilder(DensityClient client, ActionType<DecommissionResponse> action, DecommissionRequest request) {
        super(client, action, request);
    }

    /**
     * @param decommissionAttribute decommission attribute
     * @return current object
     */
    public DecommissionRequestBuilder setDecommissionedAttribute(DecommissionAttribute decommissionAttribute) {
        request.setDecommissionAttribute(decommissionAttribute);
        return this;
    }

    public DecommissionRequestBuilder setDelayTimeOut(TimeValue delayTimeOut) {
        request.setDelayTimeout(delayTimeOut);
        return this;
    }

    public DecommissionRequestBuilder setNoDelay(boolean noDelay) {
        request.setNoDelay(noDelay);
        return this;
    }

    /**
     * Sets request id for decommission request
     *
     * @param requestID for decommission request
     * @return current object
     */
    public DecommissionRequestBuilder requestID(String requestID) {
        request.setRequestID(requestID);
        return this;
    }
}
