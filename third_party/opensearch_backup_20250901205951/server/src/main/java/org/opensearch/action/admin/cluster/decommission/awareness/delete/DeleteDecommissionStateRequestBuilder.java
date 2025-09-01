/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.cluster.decommission.awareness.delete;

import org.density.action.support.clustermanager.ClusterManagerNodeOperationRequestBuilder;
import org.density.common.annotation.PublicApi;
import org.density.transport.client.DensityClient;

/**
 * Builder for Delete decommission request.
 *
 * @density.api
 */
@PublicApi(since = "2.4.0")
public class DeleteDecommissionStateRequestBuilder extends ClusterManagerNodeOperationRequestBuilder<
    DeleteDecommissionStateRequest,
    DeleteDecommissionStateResponse,
    DeleteDecommissionStateRequestBuilder> {

    public DeleteDecommissionStateRequestBuilder(DensityClient client, DeleteDecommissionStateAction action) {
        super(client, action, new DeleteDecommissionStateRequest());
    }
}
