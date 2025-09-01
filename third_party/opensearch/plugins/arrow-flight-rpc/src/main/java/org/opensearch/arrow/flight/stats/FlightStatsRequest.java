/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.arrow.flight.stats;

import org.density.action.support.nodes.BaseNodesRequest;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;
import org.density.transport.TransportRequest;

import java.io.IOException;

/**
 * Request for Flight transport statistics
 */
class FlightStatsRequest extends BaseNodesRequest<FlightStatsRequest> {

    public FlightStatsRequest(StreamInput in) throws IOException {
        super(in);
    }

    public FlightStatsRequest(String... nodeIds) {
        super(nodeIds);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }

    public static class NodeRequest extends TransportRequest {
        public NodeRequest() {}

        public NodeRequest(StreamInput in) throws IOException {
            super(in);
        }
    }
}
