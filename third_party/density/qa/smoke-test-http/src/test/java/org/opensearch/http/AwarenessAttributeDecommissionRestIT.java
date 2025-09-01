/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.http;

import org.density.action.admin.cluster.shards.routing.weighted.put.ClusterPutWeightedRoutingResponse;
import org.density.client.Request;
import org.density.client.Response;
import org.density.client.ResponseException;
import org.density.cluster.node.DiscoveryNodeRole;
import org.density.cluster.routing.WeightedRouting;
import org.density.common.settings.Settings;
import org.density.core.rest.RestStatus;
import org.density.test.DensityIntegTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.density.test.NodeRoles.onlyRole;

@DensityIntegTestCase.ClusterScope(scope = DensityIntegTestCase.Scope.TEST, numDataNodes = 0)
public class AwarenessAttributeDecommissionRestIT extends HttpSmokeTestCase{

    public void testRestStatusForDecommissioningFailedException() {
        internalCluster().startNodes(3);
        Request request = new Request("PUT", "/_cluster/decommission/awareness/zone/zone-1");
        ResponseException exception = expectThrows(
            ResponseException.class,
            () -> getRestClient().performRequest(request)
        );
        assertEquals(exception.getResponse().getStatusLine().getStatusCode(), RestStatus.BAD_REQUEST.getStatus());
        assertTrue(exception.getMessage().contains("invalid awareness attribute requested for decommissioning"));
    }

    public void testRestStatusForAcknowledgedDecommission() throws IOException {
        Settings commonSettings = Settings.builder()
            .put("cluster.routing.allocation.awareness.attributes", "zone")
            .put("cluster.routing.allocation.awareness.force.zone.values", "a,b,c")
            .build();

        logger.info("--> start 3 cluster manager nodes on zones 'a' & 'b' & 'c'");
        List<String> clusterManagerNodes = internalCluster().startNodes(
            Settings.builder()
                .put(commonSettings)
                .put("node.attr.zone", "a")
                .put(onlyRole(commonSettings, DiscoveryNodeRole.CLUSTER_MANAGER_ROLE))
                .build(),
            Settings.builder()
                .put(commonSettings)
                .put("node.attr.zone", "b")
                .put(onlyRole(commonSettings, DiscoveryNodeRole.CLUSTER_MANAGER_ROLE))
                .build(),
            Settings.builder()
                .put(commonSettings)
                .put("node.attr.zone", "c")
                .put(onlyRole(commonSettings, DiscoveryNodeRole.CLUSTER_MANAGER_ROLE))
                .build()
        );

        logger.info("--> start 3 data nodes on zones 'a' & 'b' & 'c'");
        List<String> dataNodes = internalCluster().startNodes(
            Settings.builder()
                .put(commonSettings)
                .put("node.attr.zone", "a")
                .put(onlyRole(commonSettings, DiscoveryNodeRole.DATA_ROLE))
                .build(),
            Settings.builder()
                .put(commonSettings)
                .put("node.attr.zone", "b")
                .put(onlyRole(commonSettings, DiscoveryNodeRole.DATA_ROLE))
                .build(),
            Settings.builder()
                .put(commonSettings)
                .put("node.attr.zone", "c")
                .put(onlyRole(commonSettings, DiscoveryNodeRole.DATA_ROLE))
                .build()
        );

        ensureStableCluster(6);
        logger.info("--> setting shard routing weights for weighted round robin");
        Map<String, Double> weights = Map.of("a", 1.0, "b", 1.0, "c", 0.0);
        WeightedRouting weightedRouting = new WeightedRouting("zone", weights);

        ClusterPutWeightedRoutingResponse weightedRoutingResponse = client().admin()
            .cluster()
            .prepareWeightedRouting()
            .setWeightedRouting(weightedRouting)
            .setVersion(-1)
            .get();
        assertTrue(weightedRoutingResponse.isAcknowledged());

        Request request = new Request("PUT", "/_cluster/decommission/awareness/zone/c");
        Response response = getRestClient().performRequest(request);
        assertEquals(response.getStatusLine().getStatusCode(), RestStatus.OK.getStatus());
    }
}
