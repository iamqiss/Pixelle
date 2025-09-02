/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.cluster;

import org.density.action.admin.cluster.health.ClusterHealthResponse;
import org.density.cluster.awarenesshealth.ClusterAwarenessAttributesHealth;
import org.density.cluster.node.DiscoveryNodeRole;
import org.density.common.settings.Settings;
import org.density.test.DensityIntegTestCase;

import java.util.List;
import java.util.Map;

import static org.density.test.NodeRoles.onlyRole;
import static org.hamcrest.Matchers.equalTo;

@DensityIntegTestCase.ClusterScope(scope = DensityIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ClusterAwarenessHealthIT extends DensityIntegTestCase {

    public void testAwarenessAttributeHealthSucceeded() {
        createIndex("test");
        ensureGreen();

        for (final String node : internalCluster().getNodeNames()) {
            // a very high time out, which should never fire due to the local flag
            logger.info("--> getting cluster health on [{}]", node);
            final ClusterHealthResponse health = client(node).admin()
                .cluster()
                .prepareHealth()
                .setTimeout("30s")
                .setLevel("awareness_attributes")
                .setAwarenessAttribute("zone")
                .get("10s");

            assertFalse("timed out on " + node, health.isTimedOut());
            assertThat(
                "health status on " + node,
                health.getClusterAwarenessHealth().getClusterAwarenessAttributesHealthMap().size(),
                equalTo(1)
            );
        }
    }

    public void testAwarenessAttributeHealthValidationFailed() {
        createIndex("test");
        ensureGreen();
        for (final String node : internalCluster().getNodeNames()) {
            // a very high time out, which should never fire due to the local flag
            logger.info("--> getting cluster health on [{}]", node);
            try {
                final ClusterHealthResponse health = client(node).admin()
                    .cluster()
                    .prepareHealth()
                    .setTimeout("30s")
                    .setAwarenessAttribute("zone")
                    .get("10s");
            } catch (Exception exception) {
                assertThat(
                    exception.getMessage(),
                    equalTo("Validation Failed: 1: level=awareness_attributes is required with awareness_attribute parameter;")
                );
            }
        }
    }

    public void testAwarenessAttributeHealthValidationFailedOnIndexHealth() {
        createIndex("test");
        ensureGreen();
        for (final String node : internalCluster().getNodeNames()) {
            // a very high time out, which should never fire due to the local flag
            logger.info("--> getting cluster health on [{}]", node);
            try {
                final ClusterHealthResponse health = client(node).admin()
                    .cluster()
                    .prepareHealth("test")
                    .setTimeout("30s")
                    .setLevel("awareness_attributes")
                    .setAwarenessAttribute("zone")
                    .get("10s");
            } catch (Exception exception) {
                assertThat(
                    exception.getMessage(),
                    equalTo("Validation Failed: 1: awareness_attribute is not a supported parameter with index health;")
                );
            }
        }
    }

    public void testAwarenessAttributeHealth() {
        Settings commonSettings = Settings.builder()
            .put("cluster.routing.allocation.awareness.attributes", "zone")
            .put("cluster.routing.allocation.awareness.force.zone.values", "a,b,c")
            .build();

        logger.info("--> start 3 cluster manager nodes on zones 'd' & 'e' & 'f'");
        List<String> clusterManagerNodes = internalCluster().startNodes(
            Settings.builder()
                .put(commonSettings)
                .put("node.attr.zone", "d")
                .put(onlyRole(commonSettings, DiscoveryNodeRole.CLUSTER_MANAGER_ROLE))
                .build(),
            Settings.builder()
                .put(commonSettings)
                .put("node.attr.zone", "e")
                .put(onlyRole(commonSettings, DiscoveryNodeRole.CLUSTER_MANAGER_ROLE))
                .build(),
            Settings.builder()
                .put(commonSettings)
                .put("node.attr.zone", "f")
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

        final ClusterHealthResponse health = client(dataNodes.get(0)).admin()
            .cluster()
            .prepareHealth()
            .setTimeout("30s")
            .setLevel("awareness_attributes")
            .setAwarenessAttribute("zone")
            .get("10s");

        ensureStableCluster(6);
        assertThat(health.getClusterAwarenessHealth().getClusterAwarenessAttributesHealthMap().size(), equalTo(1));
        Map<String, ClusterAwarenessAttributesHealth> attributes = health.getClusterAwarenessHealth()
            .getClusterAwarenessAttributesHealthMap();
        for (String attribute : attributes.keySet()) {
            String attributeName = attributes.get(attribute).getAwarenessAttributeName();
            assertThat(attributeName, equalTo("zone"));
            assertThat(attributes.get(attribute).getAwarenessAttributeHealthMap().size(), equalTo(3));
        }
    }

    public void testAwarenessAttributeHealthAttributeDoesNotExists() {
        Settings commonSettings = Settings.builder()
            .put("cluster.routing.allocation.awareness.attributes", "zone")
            .put("cluster.routing.allocation.awareness.force.zone.values", "a,b,c")
            .build();

        logger.info("--> start 3 cluster manager nodes on zones 'd' & 'e' & 'f'");
        List<String> clusterManagerNodes = internalCluster().startNodes(
            Settings.builder()
                .put(commonSettings)
                .put("node.attr.zone", "d")
                .put(onlyRole(commonSettings, DiscoveryNodeRole.CLUSTER_MANAGER_ROLE))
                .build(),
            Settings.builder()
                .put(commonSettings)
                .put("node.attr.zone", "e")
                .put(onlyRole(commonSettings, DiscoveryNodeRole.CLUSTER_MANAGER_ROLE))
                .build(),
            Settings.builder()
                .put(commonSettings)
                .put("node.attr.zone", "f")
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

        final ClusterHealthResponse health = client(dataNodes.get(0)).admin()
            .cluster()
            .prepareHealth()
            .setTimeout("30s")
            .setLevel("awareness_attributes")
            .setAwarenessAttribute("rack")
            .get("10s");

        ensureStableCluster(6);
        assertThat(health.getClusterAwarenessHealth().getClusterAwarenessAttributesHealthMap().size(), equalTo(1));
        Map<String, ClusterAwarenessAttributesHealth> attributes = health.getClusterAwarenessHealth()
            .getClusterAwarenessAttributesHealthMap();
        for (String attribute : attributes.keySet()) {
            String attributeName = attributes.get(attribute).getAwarenessAttributeName();
            assertThat(attributeName, equalTo("rack"));
            assertThat(attributes.get(attribute).getAwarenessAttributeHealthMap().size(), equalTo(0));
        }
    }
}
