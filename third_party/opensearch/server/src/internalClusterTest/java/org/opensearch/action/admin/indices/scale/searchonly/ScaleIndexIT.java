/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.indices.scale.searchonly;

import org.density.action.admin.indices.settings.get.GetSettingsResponse;
import org.density.action.index.IndexResponse;
import org.density.action.search.SearchResponse;
import org.density.action.support.WriteRequest;
import org.density.cluster.ClusterState;
import org.density.cluster.metadata.IndexMetadata;
import org.density.cluster.routing.IndexRoutingTable;
import org.density.cluster.routing.IndexShardRoutingTable;
import org.density.cluster.routing.ShardRouting;
import org.density.common.settings.Settings;
import org.density.core.rest.RestStatus;
import org.density.indices.replication.common.ReplicationType;
import org.density.remotestore.RemoteStoreBaseIntegTestCase;
import org.density.test.InternalTestCluster;
import org.density.test.DensityIntegTestCase;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.density.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.density.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SEARCH_REPLICAS;
import static org.density.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.density.cluster.metadata.IndexMetadata.SETTING_REPLICATION_TYPE;
import static org.density.test.hamcrest.DensityAssertions.assertAcked;
import static org.density.test.hamcrest.DensityAssertions.assertHitCount;

@DensityIntegTestCase.ClusterScope(scope = DensityIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ScaleIndexIT extends RemoteStoreBaseIntegTestCase {

    private static final String TEST_INDEX = "test_scale_index";

    public Settings indexSettings() {
        return Settings.builder().put(SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT).build();
    }

    /**
     * Tests the full lifecycle of scaling an index down to search-only mode,
     * scaling search replicas while in search-only mode, verifying cluster health in
     * various states, and then scaling back up to normal mode.
     */
    public void testFullSearchOnlyReplicasFullLifecycle() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        internalCluster().startSearchOnlyNodes(3);

        Settings specificSettings = Settings.builder()
            .put(indexSettings())
            .put(SETTING_NUMBER_OF_SHARDS, 1)
            .put(SETTING_NUMBER_OF_REPLICAS, 1)
            .put(SETTING_NUMBER_OF_SEARCH_REPLICAS, 1)
            .build();

        createIndex(TEST_INDEX, specificSettings);
        ensureGreen(TEST_INDEX);

        for (int i = 0; i < 10; i++) {
            IndexResponse indexResponse = client().prepareIndex(TEST_INDEX)
                .setId(Integer.toString(i))
                .setSource("field1", "value" + i)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();
            assertEquals(RestStatus.CREATED, indexResponse.status());
        }

        assertBusy(() -> {
            SearchResponse searchResponse = client().prepareSearch(TEST_INDEX).get();
            assertHitCount(searchResponse, 10);
            assertSearchNodeDocCounts(10, TEST_INDEX);
        }, 30, TimeUnit.SECONDS);

        ensureGreen(TEST_INDEX);

        // Scale down to search-only mode
        assertAcked(client().admin().indices().prepareScaleSearchOnly(TEST_INDEX, true).get());

        // Verify search-only setting is enabled
        GetSettingsResponse settingsResponse = client().admin().indices().prepareGetSettings(TEST_INDEX).get();
        assertTrue(settingsResponse.getSetting(TEST_INDEX, IndexMetadata.INDEX_BLOCKS_SEARCH_ONLY_SETTING.getKey()).equals("true"));

        // Verify that write operations are blocked during scale-down
        assertBusy(() -> {
            try {
                // Attempt to index a document while scale-down is in progress
                client().prepareIndex(TEST_INDEX)
                    .setId("sample-write-after-search-only-block")
                    .setSource("field1", "value1")
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .get();
                fail("Write operation should be blocked during scale-down");
            } catch (Exception e) {
                assertTrue(
                    "Exception should indicate index scaled down",
                    e.getMessage().contains("blocked by: [FORBIDDEN/20/index scaled down]")
                );
            }
        }, 10, TimeUnit.SECONDS);

        ensureGreen(TEST_INDEX);

        // Verify search still works on all search nodes
        assertSearchNodeDocCounts(10, TEST_INDEX);

        // Scale up search replicas while in search-only mode
        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings(TEST_INDEX)
                .setSettings(Settings.builder().put(SETTING_NUMBER_OF_SEARCH_REPLICAS, 3).build())
                .get()
        );

        ensureGreen(TEST_INDEX);

        // Verify search still works on all search nodes
        assertBusy(() -> { assertSearchNodeDocCounts(10, TEST_INDEX); }, 30, TimeUnit.SECONDS);

        // Scale down search replicas while still in search-only mode
        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings(TEST_INDEX)
                .setSettings(Settings.builder().put(SETTING_NUMBER_OF_SEARCH_REPLICAS, 2).build())
                .get()
        );

        ensureGreen(TEST_INDEX);

        // Verify search still works on all search nodes
        assertBusy(() -> { assertSearchNodeDocCounts(10, TEST_INDEX); }, 30, TimeUnit.SECONDS);

        // Test cluster health when one search replica is down
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(findNodesWithSearchOnlyReplicas()[0]));

        ensureYellow(TEST_INDEX);

        // Start a replacement search node and wait for recovery
        internalCluster().startSearchOnlyNode();
        ensureGreen(TEST_INDEX);

        // Scale back up to normal mode
        assertAcked(client().admin().indices().prepareScaleSearchOnly(TEST_INDEX, false).get());
        ensureGreen(TEST_INDEX);

        // Verify search-only setting is disabled
        settingsResponse = client().admin().indices().prepareGetSettings(TEST_INDEX).get();
        assertFalse(settingsResponse.getSetting(TEST_INDEX, IndexMetadata.INDEX_BLOCKS_SEARCH_ONLY_SETTING.getKey()).equals("true"));

        // Verify search still works after scale-up
        assertBusy(() -> {
            SearchResponse response = client().prepareSearch(TEST_INDEX).get();
            assertHitCount(response, 10);
            assertSearchNodeDocCounts(10, TEST_INDEX);
        }, 30, TimeUnit.SECONDS);

        // Verify writes work again after scale-up
        IndexResponse indexResponse = client().prepareIndex(TEST_INDEX)
            .setId("new-doc")
            .setSource("field1", "new-value")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        assertEquals(RestStatus.CREATED, indexResponse.status());

        // Verify new document is searchable
        assertBusy(() -> {
            SearchResponse finalResponse = client().prepareSearch(TEST_INDEX).get();
            assertHitCount(finalResponse, 11);
            assertSearchNodeDocCounts(11, TEST_INDEX);
        });
    }

    /**
     * Tests scaling down an index to search-only mode when there are no search replicas.
     */
    public void testScaleDownValidationWithoutSearchReplicas() {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        internalCluster().startSearchOnlyNode();

        Settings specificSettings = Settings.builder()
            .put(indexSettings())
            .put(SETTING_NUMBER_OF_SHARDS, 2)
            .put(SETTING_NUMBER_OF_REPLICAS, 1)
            .build();

        createIndex(TEST_INDEX, specificSettings);
        ensureYellow(TEST_INDEX);

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin().indices().prepareScaleSearchOnly(TEST_INDEX, true).get()
        );

        assertTrue(
            "Expected error about missing search replicas",
            exception.getMessage().contains("Cannot scale to zero without search replicas for index:")
        );
    }

    /**
     * Scenario 1: Tests search-only replicas recovery with persistent data directory
     * and cluster.remote_store.state.enabled=false
     */
    public void testSearchOnlyRecoveryWithPersistentData() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        internalCluster().startSearchOnlyNode();

        Settings specificSettings = Settings.builder()
            .put(indexSettings())
            .put(SETTING_NUMBER_OF_SHARDS, 2)
            .put(SETTING_NUMBER_OF_REPLICAS, 1)
            .put(SETTING_NUMBER_OF_SEARCH_REPLICAS, 1)
            .build();

        createIndex(TEST_INDEX, specificSettings);
        ensureGreen(TEST_INDEX);

        assertAcked(client().admin().indices().prepareScaleSearchOnly(TEST_INDEX, true).get());

        assertBusy(() -> {
            ClusterState state = client().admin().cluster().prepareState().get().getState();
            IndexRoutingTable routingTable = state.routingTable().index(TEST_INDEX);

            for (IndexShardRoutingTable shardTable : routingTable) {
                assertNull("Primary should be null", shardTable.primaryShard());
                assertTrue("No writer replicas should exist", shardTable.writerReplicas().isEmpty());
                assertEquals(
                    "One search replica should be active",
                    1,
                    shardTable.searchOnlyReplicas().stream().filter(ShardRouting::active).count()
                );
            }
        });
    }

    /**
     * Scenario 2: Tests behavior with cluster.remote_store.state.enabled=true
     * but without data directory preservation
     */
    public void testClusterRemoteStoreStateEnabled() throws Exception {
        Settings remoteStoreSettings = Settings.builder().put(nodeSettings(0)).put("cluster.remote_store.state.enabled", true).build();

        internalCluster().startClusterManagerOnlyNode(remoteStoreSettings);
        internalCluster().startDataOnlyNodes(2);
        internalCluster().startSearchOnlyNode();

        Settings specificSettings = Settings.builder()
            .put(indexSettings())
            .put(SETTING_NUMBER_OF_SHARDS, 2)
            .put(SETTING_NUMBER_OF_REPLICAS, 1)
            .put(SETTING_NUMBER_OF_SEARCH_REPLICAS, 1)
            .build();

        createIndex(TEST_INDEX, specificSettings);
        ensureGreen(TEST_INDEX);

        assertAcked(client().admin().indices().prepareScaleSearchOnly(TEST_INDEX, true).get());

        internalCluster().stopAllNodes();

        internalCluster().startClusterManagerOnlyNode(remoteStoreSettings);
        internalCluster().startDataOnlyNodes(2);
        internalCluster().startSearchOnlyNode();

        assertBusy(() -> {
            ClusterState state = client().admin().cluster().prepareState().get().getState();
            IndexRoutingTable routingTable = state.routingTable().index(TEST_INDEX);

            for (IndexShardRoutingTable shardTable : routingTable) {
                assertTrue(
                    "Only search replicas should be active",
                    shardTable.searchOnlyReplicas().stream().anyMatch(ShardRouting::active)
                );
            }
        });
    }

    /**
     * Scenario 3: Tests recovery with persistent data directory and remote store state
     */
    public void testRecoveryWithPersistentDataAndRemoteStore() throws Exception {
        Settings remoteStoreSettings = Settings.builder().put(nodeSettings(0)).put("cluster.remote_store.state.enabled", true).build();

        internalCluster().startClusterManagerOnlyNode(remoteStoreSettings);
        internalCluster().startDataOnlyNodes(2);
        internalCluster().startSearchOnlyNode();

        Settings specificSettings = Settings.builder()
            .put(indexSettings())
            .put(SETTING_NUMBER_OF_SHARDS, 2)
            .put(SETTING_NUMBER_OF_REPLICAS, 1)
            .put(SETTING_NUMBER_OF_SEARCH_REPLICAS, 1)
            .build();

        createIndex(TEST_INDEX, specificSettings);
        ensureGreen(TEST_INDEX);

        internalCluster().fullRestart();

        ensureGreen(TEST_INDEX);
        assertAcked(client().admin().indices().prepareScaleSearchOnly(TEST_INDEX, true).get());

        assertBusy(() -> { assertEquals("One search replica should be active", 1, findNodesWithSearchOnlyReplicas().length); });
    }

    /**
     * Helper method to find all nodes that contain active search-only replica shards
     * @return Array of node names that have active search-only replicas
     */
    private String[] findNodesWithSearchOnlyReplicas() {
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        IndexRoutingTable indexRoutingTable = state.routingTable().index(TEST_INDEX);

        Set<String> nodeNames = new HashSet<>();

        for (IndexShardRoutingTable shardTable : indexRoutingTable) {
            for (ShardRouting searchReplica : shardTable.searchOnlyReplicas()) {
                if (searchReplica.active()) {
                    nodeNames.add(state.nodes().get(searchReplica.currentNodeId()).getName());
                }
            }
        }

        if (nodeNames.isEmpty()) {
            throw new AssertionError("Could not find any node with active search-only replica");
        }

        return nodeNames.toArray(new String[0]);
    }

    /**
     * Assert that documents are accessible and have the expected count across all search nodes
     * @param expectedDocCount Expected number of documents in the index
     * @param index The index name to search
     */
    protected void assertSearchNodeDocCounts(int expectedDocCount, String index) {
        // Check on all nodes that have search-only replicas
        String[] searchNodes = findNodesWithSearchOnlyReplicas();
        for (String node : searchNodes) {
            assertHitCount(client(node).prepareSearch(index).setSize(0).get(), expectedDocCount);
        }
    }
}
