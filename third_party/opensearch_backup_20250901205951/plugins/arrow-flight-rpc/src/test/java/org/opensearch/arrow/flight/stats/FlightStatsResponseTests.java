/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.arrow.flight.stats;

import org.density.cluster.ClusterName;
import org.density.cluster.node.DiscoveryNode;
import org.density.common.io.stream.BytesStreamOutput;
import org.density.common.xcontent.XContentType;
import org.density.core.common.transport.TransportAddress;
import org.density.core.xcontent.ToXContent;
import org.density.core.xcontent.XContentBuilder;
import org.density.test.DensityTestCase;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;
import java.util.List;

public class FlightStatsResponseTests extends DensityTestCase {

    public void testBasicFunctionality() throws IOException {
        ClusterName clusterName = new ClusterName("test-cluster");
        DiscoveryNode node = new DiscoveryNode(
            "node1",
            "node1",
            new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
            Collections.emptyMap(),
            Collections.emptySet(),
            org.density.Version.CURRENT
        );
        FlightNodeStats nodeStats = new FlightNodeStats(node, new FlightMetrics());

        FlightStatsResponse response = new FlightStatsResponse(clusterName, List.of(nodeStats), Collections.emptyList());

        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);

        FlightStatsResponse deserialized = new FlightStatsResponse(out.bytes().streamInput());
        assertEquals(response.getClusterName(), deserialized.getClusterName());
    }

    public void testToXContent() throws IOException {
        ClusterName clusterName = new ClusterName("test");
        DiscoveryNode node = new DiscoveryNode(
            "node1",
            "node1",
            new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
            Collections.emptyMap(),
            Collections.emptySet(),
            org.density.Version.CURRENT
        );
        FlightNodeStats nodeStats = new FlightNodeStats(node, new FlightMetrics());
        FlightStatsResponse response = new FlightStatsResponse(clusterName, List.of(nodeStats), Collections.emptyList());

        XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent());
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String json = builder.toString();
        assertTrue(json.contains("cluster_name"));
        assertTrue(json.contains("nodes"));
    }
}
