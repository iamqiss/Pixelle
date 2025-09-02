/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.cluster.shards.routing.weighted.put;

import org.density.action.support.clustermanager.AcknowledgedResponse;
import org.density.common.annotation.PublicApi;
import org.density.core.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * Response from updating weights for weighted round-robin search routing policy.
 *
 * @density.api
 */
@PublicApi(since = "2.4.0")
public class ClusterPutWeightedRoutingResponse extends AcknowledgedResponse {
    public ClusterPutWeightedRoutingResponse(boolean acknowledged) {
        super(acknowledged);
    }

    public ClusterPutWeightedRoutingResponse(StreamInput in) throws IOException {
        super(in);
    }
}
