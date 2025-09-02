/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.cluster.awarenesshealth;

import org.density.Version;
import org.density.cluster.ClusterName;
import org.density.cluster.ClusterState;
import org.density.cluster.node.DiscoveryNode;
import org.density.cluster.node.DiscoveryNodeRole;
import org.density.cluster.node.DiscoveryNodes;
import org.density.cluster.service.ClusterService;
import org.density.common.settings.Settings;
import org.density.telemetry.tracing.noop.NoopTracer;
import org.density.test.DensityTestCase;
import org.density.test.transport.CapturingTransport;
import org.density.threadpool.TestThreadPool;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportService;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonMap;
import static org.density.test.ClusterServiceUtils.createClusterService;

public class ClusterAwarenessAttributesHealthTests extends DensityTestCase {

    private static ThreadPool threadPool;

    private ClusterService clusterService;
    private TransportService transportService;

    protected static Set<DiscoveryNodeRole> NODE_ROLE = Collections.unmodifiableSet(
        new HashSet<>(Arrays.asList(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE, DiscoveryNodeRole.DATA_ROLE))
    );

    @BeforeClass
    public static void setupThreadPool() {
        threadPool = new TestThreadPool("ClusterAwarenessAttributeHealthTests");
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        clusterService = createClusterService(threadPool);
        CapturingTransport transport = new CapturingTransport();
        transportService = transport.createTransportService(
            clusterService.getSettings(),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> clusterService.localNode(),
            null,
            Collections.emptySet(),
            NoopTracer.INSTANCE
        );
        transportService.start();
        transportService.acceptIncomingRequests();
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        clusterService.close();
        transportService.close();
    }

    @AfterClass
    public static void terminateThreadPool() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
    }

    public void testClusterAwarenessAttributeHealth() {
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .nodes(
                DiscoveryNodes.builder(clusterService.state().nodes())
                    .add(
                        new DiscoveryNode(
                            "node1",
                            buildNewFakeTransportAddress(),
                            singletonMap("zone", "zone_1"),
                            NODE_ROLE,
                            Version.CURRENT
                        )
                    )
                    .add(
                        new DiscoveryNode(
                            "node2",
                            buildNewFakeTransportAddress(),
                            singletonMap("zone", "zone_2"),
                            NODE_ROLE,
                            Version.CURRENT
                        )
                    )
                    .add(
                        new DiscoveryNode(
                            "node3",
                            buildNewFakeTransportAddress(),
                            singletonMap("zone", "zone_3"),
                            NODE_ROLE,
                            Version.CURRENT
                        )
                    )
            )
            .build();

        ClusterAwarenessAttributesHealth clusterAwarenessAttributesHealth = new ClusterAwarenessAttributesHealth(
            "zone",
            true,
            clusterState
        );

        assertEquals("zone", clusterAwarenessAttributesHealth.getAwarenessAttributeName());
        Map<String, ClusterAwarenessAttributeValueHealth> attributeValueMap = clusterAwarenessAttributesHealth
            .getAwarenessAttributeHealthMap();
        assertEquals(3, attributeValueMap.size());
    }

    public void testClusterAwarenessAttributeHealthNodeAttributeDoesNotExists() {
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .nodes(
                DiscoveryNodes.builder(clusterService.state().nodes())
                    .add(
                        new DiscoveryNode(
                            "node1",
                            buildNewFakeTransportAddress(),
                            singletonMap("zone", "zone_1"),
                            NODE_ROLE,
                            Version.CURRENT
                        )
                    )
                    .add(
                        new DiscoveryNode(
                            "node2",
                            buildNewFakeTransportAddress(),
                            singletonMap("zone", "zone_2"),
                            NODE_ROLE,
                            Version.CURRENT
                        )
                    )
                    .add(
                        new DiscoveryNode(
                            "node3",
                            buildNewFakeTransportAddress(),
                            singletonMap("zone", "zone_3"),
                            NODE_ROLE,
                            Version.CURRENT
                        )
                    )
            )
            .build();

        ClusterAwarenessAttributesHealth clusterAwarenessAttributesHealth = new ClusterAwarenessAttributesHealth(
            "rack",
            true,
            clusterState
        );

        assertEquals("rack", clusterAwarenessAttributesHealth.getAwarenessAttributeName());
        Map<String, ClusterAwarenessAttributeValueHealth> attributeValueMap = clusterAwarenessAttributesHealth
            .getAwarenessAttributeHealthMap();
        assertEquals(0, attributeValueMap.size());
    }

    public void testClusterAwarenessAttributeHealthNoReplicaEnforcement() {
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .nodes(
                DiscoveryNodes.builder(clusterService.state().nodes())
                    .add(
                        new DiscoveryNode(
                            "node1",
                            buildNewFakeTransportAddress(),
                            singletonMap("zone", "zone_1"),
                            NODE_ROLE,
                            Version.CURRENT
                        )
                    )
                    .add(
                        new DiscoveryNode(
                            "node2",
                            buildNewFakeTransportAddress(),
                            singletonMap("zone", "zone_2"),
                            NODE_ROLE,
                            Version.CURRENT
                        )
                    )
                    .add(
                        new DiscoveryNode(
                            "node3",
                            buildNewFakeTransportAddress(),
                            singletonMap("zone", "zone_3"),
                            NODE_ROLE,
                            Version.CURRENT
                        )
                    )
            )
            .build();

        ClusterAwarenessAttributesHealth clusterAwarenessAttributesHealth = new ClusterAwarenessAttributesHealth(
            "zone",
            false,
            clusterState
        );

        assertEquals("zone", clusterAwarenessAttributesHealth.getAwarenessAttributeName());
        Map<String, ClusterAwarenessAttributeValueHealth> attributeValueMap = clusterAwarenessAttributesHealth
            .getAwarenessAttributeHealthMap();
        assertEquals(3, attributeValueMap.size());
    }
}
