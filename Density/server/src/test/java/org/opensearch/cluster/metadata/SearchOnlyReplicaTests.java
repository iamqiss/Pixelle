/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.cluster.metadata;

import org.density.Version;
import org.density.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.density.action.admin.indices.create.CreateIndexRequest;
import org.density.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.density.action.support.ActiveShardCount;
import org.density.action.support.replication.ClusterStateCreationUtils;
import org.density.cluster.ClusterState;
import org.density.cluster.node.DiscoveryNode;
import org.density.cluster.node.DiscoveryNodeRole;
import org.density.cluster.routing.IndexShardRoutingTable;
import org.density.cluster.routing.ShardRoutingState;
import org.density.common.ValidationException;
import org.density.common.settings.Settings;
import org.density.env.Environment;
import org.density.gateway.remote.RemoteClusterStateService;
import org.density.indices.ShardLimitValidator;
import org.density.indices.cluster.ClusterStateChanges;
import org.density.indices.replication.common.ReplicationType;
import org.density.repositories.fs.FsRepository;
import org.density.test.DensitySingleNodeTestCase;
import org.density.threadpool.TestThreadPool;
import org.density.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.density.cluster.metadata.IndexMetadata.INDEX_REPLICATION_TYPE_SETTING;
import static org.density.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.density.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SEARCH_REPLICAS;
import static org.density.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.density.indices.IndicesService.CLUSTER_REPLICATION_TYPE_SETTING;
import static org.density.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.density.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX;
import static org.density.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT;
import static org.density.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.density.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY;

public class SearchOnlyReplicaTests extends DensitySingleNodeTestCase {

    public static final String TEST_RS_REPO = "test-rs-repo";
    public static final String INDEX_NAME = "test-index";
    private ThreadPool threadPool;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        this.threadPool = new TestThreadPool(getClass().getName());
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        terminate(threadPool);
    }

    public void testCreateWithDefaultSearchReplicasSetting() {
        final ClusterStateChanges cluster = new ClusterStateChanges(xContentRegistry(), threadPool);
        ClusterState state = createIndexWithSettings(cluster, Settings.builder().build());
        IndexShardRoutingTable indexShardRoutingTable = state.getRoutingTable().index(INDEX_NAME).getShards().get(0);
        assertEquals(1, indexShardRoutingTable.replicaShards().size());
        assertEquals(0, indexShardRoutingTable.searchOnlyReplicas().size());
        assertEquals(1, indexShardRoutingTable.writerReplicas().size());
    }

    public void testSearchReplicasValidationWithDocumentReplication() {
        final ClusterStateChanges cluster = new ClusterStateChanges(xContentRegistry(), threadPool);
        RuntimeException exception = expectThrows(
            RuntimeException.class,
            () -> createIndexWithSettings(
                cluster,
                Settings.builder()
                    .put(SETTING_NUMBER_OF_SHARDS, 1)
                    .put(SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(INDEX_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.DOCUMENT)
                    .put(SETTING_NUMBER_OF_SEARCH_REPLICAS, 1)
                    .build()
            )
        );
        assertEquals(
            "To set index.number_of_search_replicas, index.remote_store.enabled must be set to true",
            exception.getCause().getMessage()
        );
    }

    public void testUpdateSearchReplicaCount() throws ExecutionException, InterruptedException {
        Settings settings = Settings.builder()
            .put(SETTING_NUMBER_OF_SHARDS, 1)
            .put(SETTING_NUMBER_OF_REPLICAS, 0)
            .put(INDEX_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.SEGMENT)
            .put(SETTING_NUMBER_OF_SEARCH_REPLICAS, 1)
            .build();
        createIndex(INDEX_NAME, settings);

        IndexShardRoutingTable indexShardRoutingTable = getIndexShardRoutingTable();
        assertEquals(1, indexShardRoutingTable.replicaShards().size());
        assertEquals(1, indexShardRoutingTable.searchOnlyReplicas().size());
        assertEquals(0, indexShardRoutingTable.writerReplicas().size());

        // add another replica
        UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(INDEX_NAME).settings(
            Settings.builder().put(SETTING_NUMBER_OF_SEARCH_REPLICAS, 2).build()
        );
        client().admin().indices().updateSettings(updateSettingsRequest).get();
        indexShardRoutingTable = getIndexShardRoutingTable();
        assertEquals(2, indexShardRoutingTable.replicaShards().size());
        assertEquals(2, indexShardRoutingTable.searchOnlyReplicas().size());
        assertEquals(0, indexShardRoutingTable.writerReplicas().size());

        // remove all replicas
        updateSettingsRequest = new UpdateSettingsRequest(INDEX_NAME).settings(
            Settings.builder().put(SETTING_NUMBER_OF_SEARCH_REPLICAS, 0).build()
        );
        client().admin().indices().updateSettings(updateSettingsRequest).get();
        indexShardRoutingTable = getIndexShardRoutingTable();
        assertEquals(0, indexShardRoutingTable.replicaShards().size());
        assertEquals(0, indexShardRoutingTable.searchOnlyReplicas().size());
        assertEquals(0, indexShardRoutingTable.writerReplicas().size());
    }

    private IndexShardRoutingTable getIndexShardRoutingTable() {
        return client().admin().cluster().prepareState().get().getState().getRoutingTable().index(INDEX_NAME).getShards().get(0);
    }

    private ClusterState createIndexWithSettings(ClusterStateChanges cluster, Settings settings) {
        List<DiscoveryNode> allNodes = new ArrayList<>();
        // node for primary/local
        DiscoveryNode localNode = createNode(Version.CURRENT, DiscoveryNodeRole.CLUSTER_MANAGER_ROLE, DiscoveryNodeRole.DATA_ROLE);
        allNodes.add(localNode);
        // node for search replicas - we'll start with 1 and add another
        for (int i = 0; i < 2; i++) {
            allNodes.add(createNode(Version.CURRENT, DiscoveryNodeRole.CLUSTER_MANAGER_ROLE, DiscoveryNodeRole.DATA_ROLE));
        }
        ClusterState state = ClusterStateCreationUtils.state(localNode, localNode, allNodes.toArray(new DiscoveryNode[0]));

        CreateIndexRequest request = new CreateIndexRequest(INDEX_NAME, settings).waitForActiveShards(ActiveShardCount.NONE);
        state = cluster.createIndex(state, request);
        return state;
    }

    public void testUpdateSearchReplicasOverShardLimit() {
        Settings settings = Settings.builder()
            .put(SETTING_NUMBER_OF_SHARDS, 1)
            .put(SETTING_NUMBER_OF_REPLICAS, 0)
            .put(INDEX_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.SEGMENT)
            .put(SETTING_NUMBER_OF_SEARCH_REPLICAS, 0)
            .build();
        createIndex(INDEX_NAME, settings);
        Integer maxShardPerNode = ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getDefault(Settings.EMPTY);

        UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(INDEX_NAME).settings(
            Settings.builder().put(SETTING_NUMBER_OF_SEARCH_REPLICAS, maxShardPerNode * 2).build()
        );

        // add another replica
        ExecutionException executionException = expectThrows(
            ExecutionException.class,
            () -> client().admin().indices().updateSettings(updateSettingsRequest).get()
        );
        Throwable cause = executionException.getCause();
        assertEquals(ValidationException.class, cause.getClass());
    }

    public void testUpdateSearchReplicasOnDocrepCluster() {
        final ClusterStateChanges cluster = new ClusterStateChanges(xContentRegistry(), threadPool);

        List<DiscoveryNode> allNodes = new ArrayList<>();
        // node for primary/local
        DiscoveryNode localNode = createNode(Version.CURRENT, DiscoveryNodeRole.CLUSTER_MANAGER_ROLE, DiscoveryNodeRole.DATA_ROLE);
        allNodes.add(localNode);

        allNodes.add(createNode(Version.CURRENT, DiscoveryNodeRole.CLUSTER_MANAGER_ROLE, DiscoveryNodeRole.DATA_ROLE));

        ClusterState state = ClusterStateCreationUtils.state(localNode, localNode, allNodes.toArray(new DiscoveryNode[0]));

        CreateIndexRequest request = new CreateIndexRequest(
            INDEX_NAME,
            Settings.builder()
                .put(SETTING_NUMBER_OF_SHARDS, 1)
                .put(SETTING_NUMBER_OF_REPLICAS, 0)
                .put(INDEX_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.DOCUMENT)
                .build()
        ).waitForActiveShards(ActiveShardCount.NONE);
        state = cluster.createIndex(state, request);
        assertTrue(state.metadata().hasIndex(INDEX_NAME));
        rerouteUntilActive(state, cluster);

        // add another replica
        ClusterState finalState = state;
        Integer maxShardPerNode = ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getDefault(Settings.EMPTY);
        expectThrows(
            RuntimeException.class,
            () -> cluster.updateSettings(
                finalState,
                new UpdateSettingsRequest(INDEX_NAME).settings(
                    Settings.builder().put(SETTING_NUMBER_OF_SEARCH_REPLICAS, maxShardPerNode * 2).build()
                )
            )
        );

    }

    Path tempDir = createTempDir();
    Path repo = tempDir.resolve("repo");

    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
            .put(super.nodeSettings())
            .put(CLUSTER_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.SEGMENT)
            .put(buildRemoteStoreNodeAttributes(TEST_RS_REPO, repo))
            .put(Environment.PATH_HOME_SETTING.getKey(), tempDir)
            .put(Environment.PATH_REPO_SETTING.getKey(), repo)
            .build();
    }

    private Settings buildRemoteStoreNodeAttributes(String repoName, Path repoPath) {
        String repoTypeAttributeKey = String.format(
            Locale.getDefault(),
            "node.attr." + REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT,
            repoName
        );
        String repoSettingsAttributeKeyPrefix = String.format(
            Locale.getDefault(),
            "node.attr." + REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX,
            repoName
        );

        return Settings.builder()
            .put("node.attr." + REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY, repoName)
            .put(repoTypeAttributeKey, FsRepository.TYPE)
            .put(repoSettingsAttributeKeyPrefix + "location", repoPath)
            .put("node.attr." + REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY, repoName)
            .put(repoTypeAttributeKey, FsRepository.TYPE)
            .put(repoSettingsAttributeKeyPrefix + "location", repoPath)
            .put("node.attr." + REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY, repoName)
            .put(repoTypeAttributeKey, FsRepository.TYPE)
            .put(repoSettingsAttributeKeyPrefix + "location", repoPath)
            .put(RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), false)
            .build();
    }

    private static void rerouteUntilActive(ClusterState state, ClusterStateChanges cluster) {
        while (state.routingTable().index(INDEX_NAME).shard(0).allShardsStarted() == false) {
            state = cluster.applyStartedShards(
                state,
                state.routingTable().index(INDEX_NAME).shard(0).shardsWithState(ShardRoutingState.INITIALIZING)
            );
            state = cluster.reroute(state, new ClusterRerouteRequest());
        }
    }

    private static final AtomicInteger nodeIdGenerator = new AtomicInteger();

    protected DiscoveryNode createNode(Version version, DiscoveryNodeRole... mustHaveRoles) {
        Set<DiscoveryNodeRole> roles = new HashSet<>(randomSubsetOf(DiscoveryNodeRole.BUILT_IN_ROLES));
        Collections.addAll(roles, mustHaveRoles);
        final String id = String.format(Locale.ROOT, "node_%03d", nodeIdGenerator.incrementAndGet());
        return new DiscoveryNode(id, id, buildNewFakeTransportAddress(), Collections.emptyMap(), roles, version);
    }
}
