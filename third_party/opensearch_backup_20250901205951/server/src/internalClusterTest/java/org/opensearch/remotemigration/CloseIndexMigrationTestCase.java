/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.remotemigration;

import org.density.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.density.action.admin.indices.close.CloseIndexRequest;
import org.density.action.support.ActiveShardCount;
import org.density.cluster.ClusterState;
import org.density.cluster.metadata.IndexMetadata;
import org.density.cluster.metadata.MetadataIndexStateService;
import org.density.common.settings.Settings;
import org.density.indices.replication.common.ReplicationType;
import org.density.test.DensityIntegTestCase;

import java.util.concurrent.ExecutionException;

import static org.density.cluster.metadata.IndexMetadata.SETTING_REPLICATION_TYPE;
import static org.density.node.remotestore.RemoteStoreNodeService.MIGRATION_DIRECTION_SETTING;
import static org.density.node.remotestore.RemoteStoreNodeService.REMOTE_STORE_COMPATIBILITY_MODE_SETTING;
import static org.density.test.hamcrest.DensityAssertions.assertAcked;

@DensityIntegTestCase.ClusterScope(scope = DensityIntegTestCase.Scope.TEST, numDataNodes = 0)
public class CloseIndexMigrationTestCase extends MigrationBaseTestCase {
    private static final String TEST_INDEX = "ind";
    private final static String REMOTE_STORE_DIRECTION = "remote_store";
    private final static String MIXED_MODE = "mixed";

    /*
     * This test will verify the close request failure, when cluster mode is mixed
     * and migration to remote store is in progress.
     * */
    public void testFailCloseIndexWhileDocRepToRemoteStoreMigration() {
        setAddRemote(false);
        // create a docrep cluster
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().validateClusterFormed();

        // add a non-remote node
        String nonRemoteNodeName = internalCluster().startDataOnlyNode();
        internalCluster().validateClusterFormed();

        // create index in cluster
        Settings.Builder builder = Settings.builder().put(SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT);
        internalCluster().client()
            .admin()
            .indices()
            .prepareCreate(TEST_INDEX)
            .setSettings(
                builder.put("index.number_of_shards", 2)
                    .put("index.number_of_replicas", 0)
                    .put("index.routing.allocation.include._name", nonRemoteNodeName)
            )
            .setWaitForActiveShards(ActiveShardCount.ALL)
            .execute()
            .actionGet();

        // set mixed mode
        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(Settings.builder().put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), MIXED_MODE));
        assertAcked(internalCluster().client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        // add a remote node
        addRemote = true;
        internalCluster().startDataOnlyNode();
        internalCluster().validateClusterFormed();

        // set remote store migration direction
        updateSettingsRequest.persistentSettings(Settings.builder().put(MIGRATION_DIRECTION_SETTING.getKey(), REMOTE_STORE_DIRECTION));
        assertAcked(internalCluster().client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        ensureGreen(TEST_INDEX);

        // Try closing the index, expecting failure.
        ExecutionException ex = expectThrows(
            ExecutionException.class,
            () -> internalCluster().client().admin().indices().close(new CloseIndexRequest(TEST_INDEX)).get()

        );
        assertEquals("Cannot close index while remote migration is ongoing", ex.getCause().getMessage());
    }

    /*
     * Verify that index closes if compatibility mode is MIXED, and direction is set to NONE
     * */
    public void testCloseIndexRequestWithMixedCompatibilityModeAndNoDirection() {
        setAddRemote(false);
        // create a docrep cluster
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().validateClusterFormed();

        // add a non-remote node
        String nonRemoteNodeName = internalCluster().startDataOnlyNode();
        internalCluster().validateClusterFormed();

        // create index in cluster
        Settings.Builder builder = Settings.builder().put(SETTING_REPLICATION_TYPE, ReplicationType.DOCUMENT);
        internalCluster().client()
            .admin()
            .indices()
            .prepareCreate(TEST_INDEX)
            .setSettings(
                builder.put("index.number_of_shards", 2)
                    .put("index.number_of_replicas", 0)
                    .put("index.routing.allocation.include._name", nonRemoteNodeName)
            )
            .setWaitForActiveShards(ActiveShardCount.ALL)
            .execute()
            .actionGet();

        // set mixed mode
        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(Settings.builder().put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), MIXED_MODE));
        assertAcked(internalCluster().client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        ensureGreen(TEST_INDEX);

        // perform close action
        assertAcked(internalCluster().client().admin().indices().close(new CloseIndexRequest(TEST_INDEX)).actionGet());

        // verify that index has been closed
        final ClusterState clusterState = client().admin().cluster().prepareState().get().getState();

        final IndexMetadata indexMetadata = clusterState.metadata().indices().get(TEST_INDEX);
        assertEquals(IndexMetadata.State.CLOSE, indexMetadata.getState());
        final Settings indexSettings = indexMetadata.getSettings();
        assertTrue(indexSettings.hasValue(MetadataIndexStateService.VERIFIED_BEFORE_CLOSE_SETTING.getKey()));
        assertEquals(true, indexSettings.getAsBoolean(MetadataIndexStateService.VERIFIED_BEFORE_CLOSE_SETTING.getKey(), false));
        assertNotNull(clusterState.routingTable().index(TEST_INDEX));
        assertTrue(clusterState.blocks().hasIndexBlock(TEST_INDEX, MetadataIndexStateService.INDEX_CLOSED_BLOCK));

    }
}
