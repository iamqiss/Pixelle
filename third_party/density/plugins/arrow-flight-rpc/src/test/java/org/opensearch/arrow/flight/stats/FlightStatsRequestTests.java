/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.arrow.flight.stats;

import org.density.common.io.stream.BytesStreamOutput;
import org.density.test.DensityTestCase;

import java.io.IOException;

public class FlightStatsRequestTests extends DensityTestCase {

    public void testBasicFunctionality() throws IOException {
        FlightStatsRequest request = new FlightStatsRequest("node1", "node2");
        request.timeout("30s");

        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);

        FlightStatsRequest deserialized = new FlightStatsRequest(out.bytes().streamInput());
        assertArrayEquals(request.nodesIds(), deserialized.nodesIds());
    }

    public void testNodeRequest() throws IOException {
        FlightStatsRequest.NodeRequest nodeRequest = new FlightStatsRequest.NodeRequest();

        BytesStreamOutput out = new BytesStreamOutput();
        nodeRequest.writeTo(out);

        new FlightStatsRequest.NodeRequest(out.bytes().streamInput());
    }
}
