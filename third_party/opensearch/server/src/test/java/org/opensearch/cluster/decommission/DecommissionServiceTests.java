/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.cluster.decommission;

import org.density.Version;
import org.density.action.admin.cluster.decommission.awareness.delete.DeleteDecommissionStateResponse;
import org.density.action.admin.cluster.decommission.awareness.put.DecommissionRequest;
import org.density.action.admin.cluster.decommission.awareness.put.DecommissionResponse;
import org.density.cluster.ClusterName;
import org.density.cluster.ClusterState;
import org.density.cluster.coordination.CoordinationMetadata;
import org.density.cluster.metadata.Metadata;
import org.density.cluster.metadata.WeightedRoutingMetadata;
import org.density.cluster.node.DiscoveryNode;
import org.density.cluster.node.DiscoveryNodeRole;
import org.density.cluster.node.DiscoveryNodes;
import org.density.cluster.routing.WeightedRouting;
import org.density.cluster.routing.allocation.AllocationService;
import org.density.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.density.cluster.service.ClusterService;
import org.density.common.settings.ClusterSettings;
import org.density.common.settings.Settings;
import org.density.common.unit.TimeValue;
import org.density.core.action.ActionListener;
import org.density.telemetry.tracing.noop.NoopTracer;
import org.density.test.ClusterServiceUtils;
import org.density.test.DensityTestCase;
import org.density.test.transport.MockTransport;
import org.density.threadpool.TestThreadPool;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportService;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.mockito.Mockito;

import static java.util.Collections.emptySet;
import static java.util.Collections.singletonMap;
import static org.density.cluster.ClusterState.builder;
import static org.density.cluster.DensityAllocationTestCase.createAllocationService;
import static org.density.test.ClusterServiceUtils.createClusterService;
import static org.density.test.ClusterServiceUtils.setState;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

public class DecommissionServiceTests extends DensityTestCase {

    private ThreadPool threadPool;
    private ClusterService clusterService;
    private TransportService transportService;
    private AllocationService allocationService;
    private DecommissionService decommissionService;
    private ClusterSettings clusterSettings;

    @Before
    public void setUpService() {
        threadPool = new TestThreadPool("test", Settings.EMPTY);
        clusterService = createClusterService(threadPool);
        allocationService = createAllocationService();
        ClusterState clusterState = ClusterState.builder(new ClusterName("test")).build();
        logger.info("--> adding cluster manager node on zone_1");
        clusterState = addClusterManagerNodes(clusterState, "zone_1", "node1");
        logger.info("--> adding cluster manager node on zone_2");
        clusterState = addClusterManagerNodes(clusterState, "zone_2", "node6");
        logger.info("--> adding cluster manager node on zone_3");
        clusterState = addClusterManagerNodes(clusterState, "zone_3", "node11");
        logger.info("--> adding four data nodes on zone_1");
        clusterState = addDataNodes(clusterState, "zone_1", "node2", "node3", "node4", "node5");
        logger.info("--> adding four data nodes on zone_2");
        clusterState = addDataNodes(clusterState, "zone_2", "node7", "node8", "node9", "node10");
        logger.info("--> adding four data nodes on zone_3");
        clusterState = addDataNodes(clusterState, "zone_3", "node12", "node13", "node14", "node15");
        clusterState = setLocalNodeAsClusterManagerNode(clusterState, "node1");
        clusterState = setNodesInVotingConfig(
            clusterState,
            clusterState.nodes().get("node1"),
            clusterState.nodes().get("node6"),
            clusterState.nodes().get("node11")
        );
        final ClusterState.Builder builder = builder(clusterState);
        setState(clusterService, builder);
        final MockTransport transport = new MockTransport();
        transportService = transport.createTransportService(
            Settings.EMPTY,
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundTransportAddress -> clusterService.state().nodes().get("node1"),
            null,
            emptySet(),
            NoopTracer.INSTANCE
        );

        final Settings.Builder nodeSettingsBuilder = Settings.builder()
            .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.getKey(), "zone")
            .put("cluster.routing.allocation.awareness.force.zone.values", "zone_1,zone_2,zone_3");

        clusterSettings = new ClusterSettings(nodeSettingsBuilder.build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        transportService.start();
        transportService.acceptIncomingRequests();

        this.decommissionService = new DecommissionService(
            nodeSettingsBuilder.build(),
            clusterSettings,
            clusterService,
            transportService,
            threadPool,
            allocationService
        );
    }

    @After
    public void shutdownThreadPoolAndClusterService() {
        clusterService.stop();
        threadPool.shutdown();
    }

    @SuppressWarnings("unchecked")
    public void testDecommissioningNotStartedForInvalidAttributeName() throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        DecommissionAttribute decommissionAttribute = new DecommissionAttribute("rack", "rack-a");
        ActionListener<DecommissionResponse> listener = new ActionListener<DecommissionResponse>() {
            @Override
            public void onResponse(DecommissionResponse decommissionResponse) {
                fail("on response shouldn't have been called");
            }

            @Override
            public void onFailure(Exception e) {
                assertTrue(e instanceof DecommissioningFailedException);
                assertThat(e.getMessage(), Matchers.endsWith("invalid awareness attribute requested for decommissioning"));
                countDownLatch.countDown();
            }
        };
        decommissionService.startDecommissionAction(new DecommissionRequest(decommissionAttribute), listener);
        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
    }

    public void testDecommissionNotStartedWithoutWeighingAwayAttribute_1() throws InterruptedException {
        Map<String, Double> weights = Map.of("zone_1", 1.0, "zone_2", 1.0, "zone_3", 0.0);
        setWeightedRoutingWeights(weights);
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        DecommissionAttribute decommissionAttribute = new DecommissionAttribute("zone", "zone_1");
        ActionListener<DecommissionResponse> listener = new ActionListener<DecommissionResponse>() {
            @Override
            public void onResponse(DecommissionResponse decommissionResponse) {
                fail("on response shouldn't have been called");
            }

            @Override
            public void onFailure(Exception e) {
                assertTrue(e instanceof DecommissioningFailedException);
                assertThat(
                    e.getMessage(),
                    Matchers.containsString("weight for decommissioned attribute is expected to be [0.0] but found [1.0]")
                );
                countDownLatch.countDown();
            }
        };
        decommissionService.startDecommissionAction(new DecommissionRequest(decommissionAttribute), listener);
        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
    }

    public void testDecommissionNotStartedWithoutWeighingAwayAttribute_2() throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        DecommissionAttribute decommissionAttribute = new DecommissionAttribute("zone", "zone_1");
        ActionListener<DecommissionResponse> listener = new ActionListener<DecommissionResponse>() {
            @Override
            public void onResponse(DecommissionResponse decommissionResponse) {
                fail("on response shouldn't have been called");
            }

            @Override
            public void onFailure(Exception e) {
                assertTrue(e instanceof DecommissioningFailedException);
                assertThat(
                    e.getMessage(),
                    Matchers.containsString(
                        "no weights are set to the attribute. Please set appropriate weights before triggering decommission action"
                    )
                );
                countDownLatch.countDown();
            }
        };
        decommissionService.startDecommissionAction(new DecommissionRequest(decommissionAttribute), listener);
        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
    }

    public void testExternalDecommissionRetryNotAllowed() throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        DecommissionStatus oldStatus = DecommissionStatus.INIT;
        DecommissionAttributeMetadata oldMetadata = new DecommissionAttributeMetadata(
            new DecommissionAttribute("zone", "zone_1"),
            oldStatus,
            randomAlphaOfLength(10)
        );
        final ClusterState.Builder builder = builder(clusterService.state());
        setState(
            clusterService,
            builder.metadata(Metadata.builder(clusterService.state().metadata()).decommissionAttributeMetadata(oldMetadata).build())
        );
        AtomicReference<Exception> exceptionReference = new AtomicReference<>();
        ActionListener<DecommissionResponse> listener = new ActionListener<DecommissionResponse>() {
            @Override
            public void onResponse(DecommissionResponse decommissionResponse) {
                fail("on response shouldn't have been called");
                countDownLatch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                exceptionReference.set(e);
                countDownLatch.countDown();
            }
        };
        DecommissionRequest request = new DecommissionRequest(new DecommissionAttribute("zone", "zone_1"));
        decommissionService.startDecommissionAction(request, listener);
        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        MatcherAssert.assertThat("Expected onFailure to be called", exceptionReference.get(), notNullValue());
        MatcherAssert.assertThat(exceptionReference.get(), instanceOf(DecommissioningFailedException.class));
        MatcherAssert.assertThat(exceptionReference.get().getMessage(), containsString("same request is already in status [INIT]"));
    }

    @SuppressWarnings("unchecked")
    public void testDecommissioningFailedWhenAnotherAttributeDecommissioningSuccessful() throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        DecommissionStatus oldStatus = randomFrom(DecommissionStatus.SUCCESSFUL, DecommissionStatus.IN_PROGRESS, DecommissionStatus.INIT);
        DecommissionAttributeMetadata oldMetadata = new DecommissionAttributeMetadata(
            new DecommissionAttribute("zone", "zone_1"),
            oldStatus,
            randomAlphaOfLength(10)
        );
        final ClusterState.Builder builder = builder(clusterService.state());
        setState(
            clusterService,
            builder.metadata(Metadata.builder(clusterService.state().metadata()).decommissionAttributeMetadata(oldMetadata).build())
        );
        ActionListener<DecommissionResponse> listener = new ActionListener<DecommissionResponse>() {
            @Override
            public void onResponse(DecommissionResponse decommissionResponse) {
                fail("on response shouldn't have been called");
            }

            @Override
            public void onFailure(Exception e) {
                assertTrue(e instanceof DecommissioningFailedException);
                if (oldStatus.equals(DecommissionStatus.SUCCESSFUL)) {
                    assertThat(
                        e.getMessage(),
                        Matchers.endsWith("already successfully decommissioned, recommission before triggering another decommission")
                    );
                } else {
                    assertThat(e.getMessage(), Matchers.endsWith("is in progress, cannot process this request"));
                }
                countDownLatch.countDown();
            }
        };
        DecommissionRequest request = new DecommissionRequest(new DecommissionAttribute("zone", "zone_2"));
        decommissionService.startDecommissionAction(request, listener);
        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
    }

    public void testDecommissioningFailedWhenAnotherRequestForSameAttributeIsExecuted() throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        DecommissionStatus oldStatus = DecommissionStatus.INIT;
        DecommissionAttributeMetadata oldMetadata = new DecommissionAttributeMetadata(
            new DecommissionAttribute("zone", "zone_1"),
            oldStatus,
            randomAlphaOfLength(10)
        );
        final ClusterState.Builder builder = builder(clusterService.state());
        setState(
            clusterService,
            builder.metadata(Metadata.builder(clusterService.state().metadata()).decommissionAttributeMetadata(oldMetadata).build())
        );
        AtomicReference<Exception> exceptionReference = new AtomicReference<>();
        ActionListener<DecommissionResponse> listener = new ActionListener<DecommissionResponse>() {
            @Override
            public void onResponse(DecommissionResponse decommissionResponse) {
                fail("on response shouldn't have been called");
            }

            @Override
            public void onFailure(Exception e) {
                assertTrue(e instanceof DecommissioningFailedException);
                exceptionReference.set(e);
                countDownLatch.countDown();
            }
        };
        DecommissionRequest request = new DecommissionRequest(new DecommissionAttribute("zone", "zone_1"));
        decommissionService.startDecommissionAction(request, listener);
        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        assertTrue(exceptionReference.get() instanceof DecommissioningFailedException);
        assertThat(exceptionReference.get().getMessage(), Matchers.endsWith("same request is already in status [INIT]"));
    }

    public void testScheduleNodesDecommissionOnTimeout() {
        TransportService mockTransportService = Mockito.mock(TransportService.class);
        ThreadPool mockThreadPool = Mockito.mock(ThreadPool.class);
        Mockito.when(mockTransportService.getLocalNode()).thenReturn(Mockito.mock(DiscoveryNode.class));
        Mockito.when(mockTransportService.getThreadPool()).thenReturn(mockThreadPool);
        DecommissionService decommissionService = new DecommissionService(
            Settings.EMPTY,
            clusterSettings,
            clusterService,
            mockTransportService,
            threadPool,
            allocationService
        );
        DecommissionAttribute decommissionAttribute = new DecommissionAttribute("zone", "zone-2");
        DecommissionAttributeMetadata decommissionAttributeMetadata = new DecommissionAttributeMetadata(
            decommissionAttribute,
            DecommissionStatus.DRAINING,
            randomAlphaOfLength(10)
        );
        Metadata metadata = Metadata.builder().putCustom(DecommissionAttributeMetadata.TYPE, decommissionAttributeMetadata).build();
        ClusterState state = ClusterState.builder(new ClusterName("test")).metadata(metadata).build();

        DiscoveryNode decommissionedNode1 = Mockito.mock(DiscoveryNode.class);
        DiscoveryNode decommissionedNode2 = Mockito.mock(DiscoveryNode.class);

        setState(clusterService, state);
        decommissionService.scheduleNodesDecommissionOnTimeout(
            Set.of(decommissionedNode1, decommissionedNode2),
            DecommissionRequest.DEFAULT_NODE_DRAINING_TIMEOUT
        );

        Mockito.verify(mockThreadPool).schedule(Mockito.any(Runnable.class), Mockito.any(TimeValue.class), Mockito.anyString());
    }

    public void testDrainNodesWithDecommissionedAttributeWithNoDelay() {
        DecommissionAttribute decommissionAttribute = new DecommissionAttribute("zone", "zone-2");
        String requestID = randomAlphaOfLength(10);
        DecommissionAttributeMetadata decommissionAttributeMetadata = new DecommissionAttributeMetadata(
            decommissionAttribute,
            DecommissionStatus.INIT,
            requestID
        );

        Metadata metadata = Metadata.builder().putCustom(DecommissionAttributeMetadata.TYPE, decommissionAttributeMetadata).build();
        ClusterState state = ClusterState.builder(new ClusterName("test")).metadata(metadata).build();

        DecommissionRequest request = new DecommissionRequest(decommissionAttribute);
        request.setNoDelay(true);
        request.setRequestID(requestID);

        setState(clusterService, state);
        decommissionService.drainNodesWithDecommissionedAttribute(request);

    }

    public void testRecommissionAction() throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        DecommissionAttribute decommissionAttribute = new DecommissionAttribute("zone", "zone-2");
        DecommissionAttributeMetadata decommissionAttributeMetadata = new DecommissionAttributeMetadata(
            decommissionAttribute,
            DecommissionStatus.SUCCESSFUL,
            randomAlphaOfLength(10)
        );
        final ClusterState.Builder builder = builder(clusterService.state());
        setState(
            clusterService,
            builder.metadata(
                Metadata.builder(clusterService.state().metadata())
                    .decommissionAttributeMetadata(decommissionAttributeMetadata)
                    .coordinationMetadata(
                        CoordinationMetadata.builder()
                            .addVotingConfigExclusion(
                                new CoordinationMetadata.VotingConfigExclusion(clusterService.state().nodes().get("node6"))
                            )
                            .build()
                    )
                    .build()
            )
        );
        AtomicReference<ClusterState> clusterStateAtomicReference = new AtomicReference<>();

        ActionListener<DeleteDecommissionStateResponse> listener = new ActionListener<>() {
            @Override
            public void onResponse(DeleteDecommissionStateResponse decommissionResponse) {
                clusterStateAtomicReference.set(clusterService.state());
                countDownLatch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                fail("on failure shouldn't have been called");
                countDownLatch.countDown();
            }
        };
        this.decommissionService.startRecommissionAction(listener);
        // Decommission Attribute should be removed.
        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        assertNull(clusterStateAtomicReference.get().metadata().decommissionAttributeMetadata());
        assertEquals(0, clusterStateAtomicReference.get().coordinationMetadata().getVotingConfigExclusions().size());
    }

    private void setWeightedRoutingWeights(Map<String, Double> weights) {
        ClusterState clusterState = clusterService.state();
        WeightedRouting weightedRouting = new WeightedRouting("zone", weights);
        WeightedRoutingMetadata weightedRoutingMetadata = new WeightedRoutingMetadata(weightedRouting, 0);
        Metadata.Builder metadataBuilder = Metadata.builder(clusterState.metadata());
        metadataBuilder.putCustom(WeightedRoutingMetadata.TYPE, weightedRoutingMetadata);
        clusterState = ClusterState.builder(clusterState).metadata(metadataBuilder).build();
        ClusterState.Builder builder = ClusterState.builder(clusterState);
        ClusterServiceUtils.setState(clusterService, builder);
    }

    private ClusterState addDataNodes(ClusterState clusterState, String zone, String... nodeIds) {
        DiscoveryNodes.Builder nodeBuilder = DiscoveryNodes.builder(clusterState.nodes());
        List.of(nodeIds).forEach(nodeId -> nodeBuilder.add(newDataNode(nodeId, singletonMap("zone", zone))));
        clusterState = ClusterState.builder(clusterState).nodes(nodeBuilder).build();
        return clusterState;
    }

    private ClusterState addClusterManagerNodes(ClusterState clusterState, String zone, String... nodeIds) {
        DiscoveryNodes.Builder nodeBuilder = DiscoveryNodes.builder(clusterState.nodes());
        List.of(nodeIds).forEach(nodeId -> nodeBuilder.add(newClusterManagerNode(nodeId, singletonMap("zone", zone))));
        clusterState = ClusterState.builder(clusterState).nodes(nodeBuilder).build();
        return clusterState;
    }

    private ClusterState setLocalNodeAsClusterManagerNode(ClusterState clusterState, String nodeId) {
        DiscoveryNodes.Builder nodeBuilder = DiscoveryNodes.builder(clusterState.nodes());
        nodeBuilder.localNodeId(nodeId);
        nodeBuilder.clusterManagerNodeId(nodeId);
        clusterState = ClusterState.builder(clusterState).nodes(nodeBuilder).build();
        return clusterState;
    }

    private ClusterState setNodesInVotingConfig(ClusterState clusterState, DiscoveryNode... nodes) {
        final CoordinationMetadata.VotingConfiguration votingConfiguration = CoordinationMetadata.VotingConfiguration.of(nodes);

        Metadata.Builder builder = Metadata.builder()
            .coordinationMetadata(
                CoordinationMetadata.builder()
                    .lastAcceptedConfiguration(votingConfiguration)
                    .lastCommittedConfiguration(votingConfiguration)
                    .build()
            );
        clusterState = ClusterState.builder(clusterState).metadata(builder).build();
        return clusterState;
    }

    private static DiscoveryNode newDataNode(String nodeId, Map<String, String> attributes) {
        return new DiscoveryNode(nodeId, buildNewFakeTransportAddress(), attributes, DATA_ROLE, Version.CURRENT);
    }

    private static DiscoveryNode newClusterManagerNode(String nodeId, Map<String, String> attributes) {
        return new DiscoveryNode(nodeId, buildNewFakeTransportAddress(), attributes, CLUSTER_MANAGER_ROLE, Version.CURRENT);
    }

    final private static Set<DiscoveryNodeRole> CLUSTER_MANAGER_ROLE = Collections.unmodifiableSet(
        new HashSet<>(Collections.singletonList(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE))
    );

    final private static Set<DiscoveryNodeRole> DATA_ROLE = Collections.unmodifiableSet(
        new HashSet<>(Collections.singletonList(DiscoveryNodeRole.DATA_ROLE))
    );
}
