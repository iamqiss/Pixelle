/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.arrow.flight.stats;

import org.density.action.support.nodes.BaseNodeResponse;
import org.density.cluster.node.DiscoveryNode;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Flight transport statistics for a single node
 */
class FlightNodeStats extends BaseNodeResponse {

    private final FlightMetrics metrics;

    public FlightNodeStats(StreamInput in) throws IOException {
        super(in);
        this.metrics = new FlightMetrics(in);
    }

    public FlightNodeStats(DiscoveryNode node, FlightMetrics metrics) {
        super(node);
        this.metrics = metrics;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        metrics.writeTo(out);
    }

    public FlightMetrics getMetrics() {
        return metrics;
    }
}
