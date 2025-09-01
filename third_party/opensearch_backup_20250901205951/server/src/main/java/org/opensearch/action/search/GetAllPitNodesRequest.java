/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.search;

import org.density.action.support.nodes.BaseNodesRequest;
import org.density.cluster.node.DiscoveryNode;
import org.density.common.annotation.PublicApi;
import org.density.common.inject.Inject;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Request to get all active PIT IDs from all nodes of cluster
 *
 * @density.api
 */
@PublicApi(since = "2.3.0")
public class GetAllPitNodesRequest extends BaseNodesRequest<GetAllPitNodesRequest> {

    @Inject
    public GetAllPitNodesRequest(DiscoveryNode... concreteNodes) {
        super(concreteNodes);
    }

    public GetAllPitNodesRequest(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }
}
