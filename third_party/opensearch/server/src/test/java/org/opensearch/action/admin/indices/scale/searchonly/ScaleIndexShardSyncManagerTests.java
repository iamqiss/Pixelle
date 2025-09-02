/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.indices.scale.searchonly;

import org.density.Version;
import org.density.cluster.ClusterName;
import org.density.cluster.ClusterState;
import org.density.cluster.metadata.IndexMetadata;
import org.density.cluster.node.DiscoveryNode;
import org.density.cluster.node.DiscoveryNodes;
import org.density.cluster.routing.IndexRoutingTable;
import org.density.cluster.routing.IndexShardRoutingTable;
import org.density.cluster.routing.RoutingTable;
import org.density.cluster.routing.ShardRouting;
import org.density.cluster.routing.ShardRoutingState;
import org.density.cluster.routing.TestShardRouting;
import org.density.cluster.service.ClusterService;
import org.density.common.settings.Settings;
import org.density.core.action.ActionListener;
import org.density.core.index.Index;
import org.density.core.index.shard.ShardId;
import org.density.test.DensityTestCase;
import org.density.transport.TransportException;
import org.density.transport.TransportResponseHandler;
import org.density.transport.TransportService;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ScaleIndexShardSyncManagerTests extends DensityTestCase {

    private ClusterService clusterService;
    private TransportService transportService;
    private ScaleIndexShardSyncManager syncManager;
    private final String transportActionName = "dummyAction";

    @Override
    public void setUp() throws Exception {
        super.setUp();
        clusterService = mock(ClusterService.class);
        transportService = mock(TransportService.class);
        syncManager = new ScaleIndexShardSyncManager(clusterService, transportService, transportActionName);
    }

    public void testSendShardSyncRequests_emptyPrimaryShards() {
        ActionListener<Collection<ScaleIndexNodeResponse>> listener = new ActionListener<>() {
            @Override
            public void onResponse(Collection<ScaleIndexNodeResponse> responses) {
                fail("Expected failure when primary shards map is empty");
            }

            @Override
            public void onFailure(Exception e) {
                assertTrue(e instanceof IllegalStateException);
                assertEquals("No primary shards found for index test_index", e.getMessage());
            }
        };
        syncManager.sendShardSyncRequests("test_index", Collections.emptyMap(), listener);
    }

    public void testSendShardSyncRequests_nodeNotFound() {
        // Prepare a mapping: one shard assigned to node "node1"
        ShardId shardId = new ShardId(new Index("test_index", "uuid"), 0);
        Map<ShardId, String> primaryShardsNodes = Collections.singletonMap(shardId, "node1");

        // Set cluster state with empty discovery nodes so "node1" is missing.
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().build();
        ClusterState clusterState = ClusterState.builder(new ClusterName("test")).nodes(discoveryNodes).build();
        when(clusterService.state()).thenReturn(clusterState);

        ActionListener<Collection<ScaleIndexNodeResponse>> listener = new ActionListener<>() {
            @Override
            public void onResponse(Collection<ScaleIndexNodeResponse> responses) {
                fail("Expected failure due to missing node");
            }

            @Override
            public void onFailure(Exception e) {
                assertTrue(e.getMessage().contains("Node [node1] not found"));
            }
        };

        syncManager.sendShardSyncRequests("test_index", primaryShardsNodes, listener);
    }

    public void testSendShardSyncRequests_success() throws Exception {
        // Prepare a mapping: one shard assigned to node "node1"
        ShardId shardId = new ShardId(new Index("test_index", "uuid"), 0);
        Map<ShardId, String> primaryShardsNodes = Collections.singletonMap(shardId, "node1");

        // Build cluster state with discovery node "node1"
        DiscoveryNode node = new DiscoveryNode("node1", buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().add(node).localNodeId("node1").build();
        ClusterState clusterState = ClusterState.builder(new ClusterName("test")).nodes(discoveryNodes).build();
        when(clusterService.state()).thenReturn(clusterState);

        // Stub transportService.sendRequest to return a dummy response.
        doAnswer(invocation -> {
            TransportResponseHandler<ScaleIndexNodeResponse> handler = invocation.getArgument(3);
            handler.handleResponse(new ScaleIndexNodeResponse(node, Collections.emptyList()));
            return null;
        }).when(transportService)
            .sendRequest(
                any(DiscoveryNode.class),
                eq(transportActionName),
                any(ScaleIndexNodeRequest.class),
                any(TransportResponseHandler.class)
            );

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Collection<ScaleIndexNodeResponse>> responseRef = new AtomicReference<>();
        AtomicReference<Exception> exceptionRef = new AtomicReference<>();

        ActionListener<Collection<ScaleIndexNodeResponse>> listener = new ActionListener<>() {
            @Override
            public void onResponse(Collection<ScaleIndexNodeResponse> responses) {
                responseRef.set(responses);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                exceptionRef.set(e);
                latch.countDown();
            }
        };

        syncManager.sendShardSyncRequests("test_index", primaryShardsNodes, listener);
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertNull(exceptionRef.get());
        Collection<ScaleIndexNodeResponse> responses = responseRef.get();
        assertNotNull(responses);
        // We expect one response since there's one node.
        assertEquals(1, responses.size());
    }

    public void testSendNodeRequest_success() throws Exception {
        DiscoveryNode node = new DiscoveryNode("node1", buildNewFakeTransportAddress(), Version.CURRENT);
        String index = "test_index";
        List<ShardId> shards = Collections.singletonList(new ShardId(new Index("test_index", "uuid"), 0));

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<ScaleIndexNodeResponse> responseRef = new AtomicReference<>();

        ActionListener<ScaleIndexNodeResponse> listener = new ActionListener<>() {
            @Override
            public void onResponse(ScaleIndexNodeResponse response) {
                responseRef.set(response);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                fail("Unexpected failure: " + e.getMessage());
            }
        };

        doAnswer(invocation -> {
            TransportResponseHandler<ScaleIndexNodeResponse> handler = invocation.getArgument(3);
            handler.handleResponse(new ScaleIndexNodeResponse(node, Collections.emptyList()));
            return null;
        }).when(transportService)
            .sendRequest(eq(node), eq(transportActionName), any(ScaleIndexNodeRequest.class), any(TransportResponseHandler.class));

        syncManager.sendNodeRequest(node, index, shards, listener);
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertNotNull(responseRef.get());
    }

    public void testSendNodeRequest_failure() throws Exception {
        DiscoveryNode node = new DiscoveryNode("node1", buildNewFakeTransportAddress(), Version.CURRENT);
        String index = "test_index";
        List<ShardId> shards = Collections.singletonList(new ShardId(new Index("test_index", "uuid"), 0));

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> exceptionRef = new AtomicReference<>();

        ActionListener<ScaleIndexNodeResponse> listener = new ActionListener<>() {
            @Override
            public void onResponse(ScaleIndexNodeResponse response) {
                fail("Expected failure");
            }

            @Override
            public void onFailure(Exception e) {
                exceptionRef.set(e);
                latch.countDown();
            }
        };

        // Use a dummy Throwable as the cause instead of passing the node.
        doAnswer(invocation -> {
            TransportResponseHandler<ScaleIndexNodeResponse> handler = invocation.getArgument(3);
            handler.handleException(new TransportException("Test exception", new Exception("dummy cause")));
            return null;
        }).when(transportService)
            .sendRequest(eq(node), eq(transportActionName), any(ScaleIndexNodeRequest.class), any(TransportResponseHandler.class));

        syncManager.sendNodeRequest(node, index, shards, listener);
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertNotNull(exceptionRef.get());
        assertTrue(exceptionRef.get() instanceof TransportException);
    }

    public void testValidateNodeResponses_success() {
        // Create a shard response with no failures.
        ShardId shardId = new ShardId(new Index("test_index", "uuid"), 0);
        ScaleIndexShardResponse shardResponse = new ScaleIndexShardResponse(shardId, false, 0);
        ScaleIndexNodeResponse nodeResponse = new ScaleIndexNodeResponse(
            new DiscoveryNode("node1", buildNewFakeTransportAddress(), Version.CURRENT),
            Collections.singletonList(shardResponse)
        );

        List<ScaleIndexNodeResponse> responses = Collections.singletonList(nodeResponse);
        AtomicReference<ScaleIndexResponse> responseRef = new AtomicReference<>();
        AtomicReference<Exception> exceptionRef = new AtomicReference<>();

        syncManager.validateNodeResponses(responses, new ActionListener<ScaleIndexResponse>() {
            @Override
            public void onResponse(ScaleIndexResponse response) {
                responseRef.set(response);
            }

            @Override
            public void onFailure(Exception e) {
                exceptionRef.set(e);
            }
        });

        assertNull(exceptionRef.get());
        assertNotNull(responseRef.get());
    }

    public void testValidateNodeResponses_failure_uncommitted() {
        // Create a shard response indicating uncommitted operations.
        ShardId shardId = new ShardId(new Index("test_index", "uuid"), 0);
        ScaleIndexShardResponse shardResponse = new ScaleIndexShardResponse(shardId, false, 5);
        ScaleIndexNodeResponse nodeResponse = new ScaleIndexNodeResponse(
            new DiscoveryNode("node1", buildNewFakeTransportAddress(), Version.CURRENT),
            Collections.singletonList(shardResponse)
        );

        List<ScaleIndexNodeResponse> responses = Collections.singletonList(nodeResponse);
        AtomicReference<Exception> exceptionRef = new AtomicReference<>();

        syncManager.validateNodeResponses(responses, new ActionListener<ScaleIndexResponse>() {
            @Override
            public void onResponse(ScaleIndexResponse response) {
                fail("Expected failure due to uncommitted operations");
            }

            @Override
            public void onFailure(Exception e) {
                exceptionRef.set(e);
            }
        });

        assertNotNull(exceptionRef.get());
        assertTrue(exceptionRef.get().getMessage().contains("uncommitted operations"));
    }

    public void testValidateNodeResponses_failure_needsSync() {
        // Create a shard response indicating that a shard needs sync.
        ShardId shardId = new ShardId(new Index("test_index", "uuid"), 0);
        ScaleIndexShardResponse shardResponse = new ScaleIndexShardResponse(shardId, true, 0);
        ScaleIndexNodeResponse nodeResponse = new ScaleIndexNodeResponse(
            new DiscoveryNode("node1", buildNewFakeTransportAddress(), Version.CURRENT),
            Collections.singletonList(shardResponse)
        );

        List<ScaleIndexNodeResponse> responses = Collections.singletonList(nodeResponse);
        AtomicReference<Exception> exceptionRef = new AtomicReference<>();

        syncManager.validateNodeResponses(responses, new ActionListener<ScaleIndexResponse>() {
            @Override
            public void onResponse(ScaleIndexResponse response) {
                fail("Expected failure due to sync needed");
            }

            @Override
            public void onFailure(Exception e) {
                exceptionRef.set(e);
            }
        });

        assertNotNull(exceptionRef.get());
        assertTrue(exceptionRef.get().getMessage().contains("sync needed"));
    }

    public void testGetPrimaryShardAssignments_withRouting() {
        // Create index settings with an explicit uuid.
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put("index.uuid", "uuid")
            .build();
        // Build IndexMetadata using the index name. The builder will pick up the uuid from the settings.
        IndexMetadata indexMetadata = IndexMetadata.builder("test_index")
            .settings(indexSettings)
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();

        // Build a minimal routing table using the same index name and uuid.
        Index index = new Index("test_index", "uuid");
        ShardId shardId = new ShardId(index, 0);
        ShardRouting primaryShardRouting = TestShardRouting.newShardRouting(shardId, "node1", true, ShardRoutingState.STARTED);
        IndexShardRoutingTable shardRoutingTable = new IndexShardRoutingTable.Builder(shardId).addShard(primaryShardRouting).build();
        IndexRoutingTable indexRoutingTable = new IndexRoutingTable.Builder(index).addIndexShard(shardRoutingTable).build();
        RoutingTable routingTable = RoutingTable.builder().add(indexRoutingTable).build();

        // Build a cluster state that contains the routing table.
        ClusterState state = ClusterState.builder(new ClusterName("test")).routingTable(routingTable).build();

        // Invoke the method under test.
        Map<ShardId, String> assignments = syncManager.getPrimaryShardAssignments(indexMetadata, state);
        // We expect one mapping: shard0 -> "node1"
        assertEquals(1, assignments.size());
        // Construct the expected shard id using the same Index (name and uuid).
        ShardId expectedShardId = new ShardId(new Index("test_index", "uuid"), 0);
        assertEquals("node1", assignments.get(expectedShardId));
    }

}
