/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.cluster.awarenesshealth;

import org.density.Version;
import org.density.action.support.IndicesOptions;
import org.density.cluster.ClusterName;
import org.density.cluster.ClusterState;
import org.density.cluster.metadata.IndexMetadata;
import org.density.cluster.metadata.IndexNameExpressionResolver;
import org.density.cluster.metadata.Metadata;
import org.density.cluster.node.DiscoveryNode;
import org.density.cluster.node.DiscoveryNodeRole;
import org.density.cluster.node.DiscoveryNodes;
import org.density.cluster.routing.IndexRoutingTable;
import org.density.cluster.routing.RoutingTable;
import org.density.cluster.routing.RoutingTableGenerator;
import org.density.cluster.routing.allocation.AwarenessReplicaBalance;
import org.density.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.density.cluster.service.ClusterService;
import org.density.common.io.stream.BytesStreamOutput;
import org.density.common.settings.ClusterSettings;
import org.density.common.settings.Settings;
import org.density.common.util.concurrent.ThreadContext;
import org.density.core.common.io.stream.StreamInput;
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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonMap;
import static org.density.test.ClusterServiceUtils.createClusterService;

public class ClusterAwarenessHealthTests extends DensityTestCase {

    private final IndexNameExpressionResolver indexNameExpressionResolver = new IndexNameExpressionResolver(
        new ThreadContext(Settings.EMPTY)
    );

    private static ThreadPool threadPool;

    private ClusterService clusterService;
    private TransportService transportService;

    protected static Set<DiscoveryNodeRole> NODE_ROLE = Collections.unmodifiableSet(
        new HashSet<>(Arrays.asList(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE, DiscoveryNodeRole.DATA_ROLE))
    );

    @BeforeClass
    public static void setupThreadPool() {
        threadPool = new TestThreadPool("ClusterAwarenessHealthTests");
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

    public void testClusterHealthWithNoAwarenessAttribute() throws IOException {
        RoutingTableGenerator routingTableGenerator = new RoutingTableGenerator();
        RoutingTableGenerator.ShardCounter counter = new RoutingTableGenerator.ShardCounter();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        Metadata.Builder metadata = Metadata.builder();
        for (int i = 4; i >= 0; i--) {
            int numberOfShards = randomInt(3) + 1;
            int numberOfReplicas = randomInt(4);
            IndexMetadata indexMetadata = IndexMetadata.builder("test_" + i)
                .settings(settings(Version.CURRENT))
                .numberOfShards(numberOfShards)
                .numberOfReplicas(numberOfReplicas)
                .build();
            IndexRoutingTable indexRoutingTable = routingTableGenerator.genIndexRoutingTable(indexMetadata, counter);
            metadata.put(indexMetadata, true);
            routingTable.add(indexRoutingTable);
        }

        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable.build())
            .nodes(clusterService.state().nodes())
            .build();

        indexNameExpressionResolver.concreteIndexNames(clusterState, IndicesOptions.strictExpand(), (String[]) null);

        ClusterAwarenessHealth clusterAwarenessHealth = new ClusterAwarenessHealth(clusterState, clusterSettings, "zone");
        clusterAwarenessHealth = serializeResponse(clusterAwarenessHealth);
        Map<String, ClusterAwarenessAttributesHealth> attributeHealthMap = clusterAwarenessHealth.getClusterAwarenessAttributesHealthMap();
        for (String attributesName : attributeHealthMap.keySet()) {
            assertEquals("zone", attributesName);
            assertEquals(0, attributeHealthMap.get(attributesName).getAwarenessAttributeHealthMap().size());
        }
    }

    ClusterAwarenessHealth serializeResponse(ClusterAwarenessHealth clusterAwarenessHealth) throws IOException {
        if (randomBoolean()) {
            BytesStreamOutput out = new BytesStreamOutput();
            clusterAwarenessHealth.writeTo(out);
            StreamInput in = out.bytes().streamInput();
            clusterAwarenessHealth = new ClusterAwarenessHealth(in);
        }
        return clusterAwarenessHealth;
    }

    public void testClusterHealthWithAwarenessAttributeAndNoNodeAttribute() throws IOException {
        RoutingTableGenerator routingTableGenerator = new RoutingTableGenerator();
        RoutingTableGenerator.ShardCounter counter = new RoutingTableGenerator.ShardCounter();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        Metadata.Builder metadata = Metadata.builder();
        for (int i = 4; i >= 0; i--) {
            int numberOfShards = randomInt(3) + 1;
            int numberOfReplicas = randomInt(4);
            IndexMetadata indexMetadata = IndexMetadata.builder("test_" + i)
                .settings(settings(Version.CURRENT))
                .numberOfShards(numberOfShards)
                .numberOfReplicas(numberOfReplicas)
                .build();
            IndexRoutingTable indexRoutingTable = routingTableGenerator.genIndexRoutingTable(indexMetadata, counter);
            metadata.put(indexMetadata, true);
            routingTable.add(indexRoutingTable);
        }

        Settings settings = Settings.builder()
            .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.getKey(), "zone")
            .build();

        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable.build())
            .nodes(clusterService.state().nodes())
            .build();

        indexNameExpressionResolver.concreteIndexNames(clusterState, IndicesOptions.strictExpand(), (String[]) null);

        ClusterAwarenessHealth clusterAwarenessHealth = new ClusterAwarenessHealth(clusterState, clusterSettings, "zone");
        clusterAwarenessHealth = serializeResponse(clusterAwarenessHealth);
        Map<String, ClusterAwarenessAttributesHealth> attributeHealthMap = clusterAwarenessHealth.getClusterAwarenessAttributesHealthMap();
        for (String attributesName : attributeHealthMap.keySet()) {
            assertEquals("zone", attributesName);
            assertEquals(0, attributeHealthMap.get(attributesName).getAwarenessAttributeHealthMap().size());
        }
    }

    public void testClusterHealthWithAwarenessAttributeAndNodeAttribute() throws IOException {
        RoutingTableGenerator routingTableGenerator = new RoutingTableGenerator();
        RoutingTableGenerator.ShardCounter counter = new RoutingTableGenerator.ShardCounter();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        Metadata.Builder metadata = Metadata.builder();
        for (int i = 4; i >= 0; i--) {
            int numberOfShards = randomInt(3) + 1;
            int numberOfReplicas = randomInt(4);
            IndexMetadata indexMetadata = IndexMetadata.builder("test_" + i)
                .settings(settings(Version.CURRENT))
                .numberOfShards(numberOfShards)
                .numberOfReplicas(numberOfReplicas)
                .build();
            IndexRoutingTable indexRoutingTable = routingTableGenerator.genIndexRoutingTable(indexMetadata, counter);
            metadata.put(indexMetadata, true);
            routingTable.add(indexRoutingTable);
        }

        Settings settings = Settings.builder()
            .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.getKey(), "zone")
            .build();

        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable.build())
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

        indexNameExpressionResolver.concreteIndexNames(clusterState, IndicesOptions.strictExpand(), (String[]) null);

        ClusterAwarenessHealth clusterAwarenessHealth = new ClusterAwarenessHealth(clusterState, clusterSettings, "zone");
        clusterAwarenessHealth = serializeResponse(clusterAwarenessHealth);
        Map<String, ClusterAwarenessAttributesHealth> attributeHealthMap = clusterAwarenessHealth.getClusterAwarenessAttributesHealthMap();
        for (String attributesName : attributeHealthMap.keySet()) {
            assertEquals("zone", attributesName);
            assertEquals(3, attributeHealthMap.get(attributesName).getAwarenessAttributeHealthMap().size());
            for (ClusterAwarenessAttributeValueHealth clusterAwarenessAttributeValueHealth : attributeHealthMap.get(attributesName)
                .getAwarenessAttributeHealthMap()
                .values()) {
                assertEquals(-1, clusterAwarenessAttributeValueHealth.getUnassignedShards());
            }
        }
    }

    public void testClusterHealthWithAwarenessAttributeAndReplicaEnforcementEnabled() throws IOException {
        RoutingTableGenerator routingTableGenerator = new RoutingTableGenerator();
        RoutingTableGenerator.ShardCounter counter = new RoutingTableGenerator.ShardCounter();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        Metadata.Builder metadata = Metadata.builder();
        for (int i = 4; i >= 0; i--) {
            int numberOfShards = 1;
            int numberOfReplicas = 5;
            IndexMetadata indexMetadata = IndexMetadata.builder("test_" + i)
                .settings(settings(Version.CURRENT))
                .numberOfShards(numberOfShards)
                .numberOfReplicas(numberOfReplicas)
                .build();
            IndexRoutingTable indexRoutingTable = routingTableGenerator.genIndexRoutingTable(indexMetadata, counter);
            metadata.put(indexMetadata, true);
            routingTable.add(indexRoutingTable);
        }

        Settings settings = Settings.builder()
            .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING.getKey() + "zone" + ".values", "a,b,c")
            .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.getKey(), "zone")
            .put(AwarenessReplicaBalance.CLUSTER_ROUTING_ALLOCATION_AWARENESS_BALANCE_SETTING.getKey(), "true")
            .build();

        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable.build())
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

        indexNameExpressionResolver.concreteIndexNames(clusterState, IndicesOptions.strictExpand(), (String[]) null);

        ClusterAwarenessHealth clusterAwarenessHealth = new ClusterAwarenessHealth(clusterState, clusterSettings, "zone");
        clusterAwarenessHealth = serializeResponse(clusterAwarenessHealth);
        Map<String, ClusterAwarenessAttributesHealth> attributeHealthMap = clusterAwarenessHealth.getClusterAwarenessAttributesHealthMap();
        for (String attributesName : attributeHealthMap.keySet()) {
            assertEquals("zone", attributesName);
            assertEquals(3, attributeHealthMap.get(attributesName).getAwarenessAttributeHealthMap().size());
            for (ClusterAwarenessAttributeValueHealth clusterAwarenessAttributeValueHealth : attributeHealthMap.get(attributesName)
                .getAwarenessAttributeHealthMap()
                .values()) {
                assertEquals(1.0, clusterAwarenessAttributeValueHealth.getWeight(), 0.0);
            }
        }
    }

    public void testClusterHealthWithAwarenessAttributeAndReplicaEnforcementNotEnabled() throws IOException {
        RoutingTableGenerator routingTableGenerator = new RoutingTableGenerator();
        RoutingTableGenerator.ShardCounter counter = new RoutingTableGenerator.ShardCounter();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        Metadata.Builder metadata = Metadata.builder();
        for (int i = 4; i >= 0; i--) {
            int numberOfShards = randomInt(3) + 1;
            int numberOfReplicas = randomInt(4);
            IndexMetadata indexMetadata = IndexMetadata.builder("test_" + i)
                .settings(settings(Version.CURRENT))
                .numberOfShards(numberOfShards)
                .numberOfReplicas(numberOfReplicas)
                .build();
            IndexRoutingTable indexRoutingTable = routingTableGenerator.genIndexRoutingTable(indexMetadata, counter);
            metadata.put(indexMetadata, true);
            routingTable.add(indexRoutingTable);
        }

        Settings settings = Settings.builder()
            .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.getKey(), "zone")
            .put(AwarenessReplicaBalance.CLUSTER_ROUTING_ALLOCATION_AWARENESS_BALANCE_SETTING.getKey(), "true")
            .build();

        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable.build())
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

        indexNameExpressionResolver.concreteIndexNames(clusterState, IndicesOptions.strictExpand(), (String[]) null);

        ClusterAwarenessHealth clusterAwarenessHealth = new ClusterAwarenessHealth(clusterState, clusterSettings, "zone");
        clusterAwarenessHealth = serializeResponse(clusterAwarenessHealth);
        Map<String, ClusterAwarenessAttributesHealth> attributeHealthMap = clusterAwarenessHealth.getClusterAwarenessAttributesHealthMap();
        for (String attributesName : attributeHealthMap.keySet()) {
            assertEquals("zone", attributesName);
            assertEquals(3, attributeHealthMap.get(attributesName).getAwarenessAttributeHealthMap().size());
            for (ClusterAwarenessAttributeValueHealth clusterAwarenessAttributeValueHealth : attributeHealthMap.get(attributesName)
                .getAwarenessAttributeHealthMap()
                .values()) {
                assertEquals(-1, clusterAwarenessAttributeValueHealth.getUnassignedShards());
                assertEquals(1.0, clusterAwarenessAttributeValueHealth.getWeight(), 0.0);
            }
        }
    }
}
