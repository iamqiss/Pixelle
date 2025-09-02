/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.remotemigration;

import org.density.action.admin.cluster.health.ClusterHealthRequest;
import org.density.action.admin.cluster.health.ClusterHealthResponse;
import org.density.action.admin.cluster.repositories.get.GetRepositoriesRequest;
import org.density.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.density.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.density.cluster.routing.allocation.command.MoveAllocationCommand;
import org.density.common.Priority;
import org.density.common.settings.Settings;
import org.density.common.unit.TimeValue;
import org.density.index.query.QueryBuilders;
import org.density.indices.recovery.PeerRecoveryTargetService;
import org.density.indices.recovery.RecoverySettings;
import org.density.plugins.Plugin;
import org.density.test.DensityIntegTestCase;
import org.density.test.hamcrest.DensityAssertions;
import org.density.test.transport.MockTransportService;
import org.density.transport.TransportService;
import org.density.transport.client.Client;
import org.density.transport.client.Requests;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Arrays.asList;
import static org.density.node.remotestore.RemoteStoreNodeService.MIGRATION_DIRECTION_SETTING;
import static org.density.node.remotestore.RemoteStoreNodeService.REMOTE_STORE_COMPATIBILITY_MODE_SETTING;
import static org.density.test.hamcrest.DensityAssertions.assertAcked;

@DensityIntegTestCase.ClusterScope(scope = DensityIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemotePrimaryRelocationIT extends MigrationBaseTestCase {
    protected int maximumNumberOfShards() {
        return 1;
    }

    protected int maximumNumberOfReplicas() {
        return 0;
    }

    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return asList(MockTransportService.TestPlugin.class);
    }

    public void testRemotePrimaryRelocation() throws Exception {
        List<String> docRepNodes = internalCluster().startNodes(2);
        Client client = internalCluster().client(docRepNodes.get(0));
        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(Settings.builder().put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), "mixed"));
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        // create shard with 0 replica and 1 shard
        client().admin().indices().prepareCreate("test").setSettings(indexSettings()).setMapping("field", "type=text").get();
        ensureGreen("test");

        AtomicInteger numAutoGenDocs = new AtomicInteger();
        final AtomicBoolean finished = new AtomicBoolean(false);
        AsyncIndexingService asyncIndexingService = new AsyncIndexingService("test");
        asyncIndexingService.startIndexing();
        refresh("test");

        // add remote node in mixed mode cluster
        setAddRemote(true);
        String remoteNode = internalCluster().startNode();
        internalCluster().validateClusterFormed();

        updateSettingsRequest.persistentSettings(Settings.builder().put(MIGRATION_DIRECTION_SETTING.getKey(), "remote_store"));
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        String remoteNode2 = internalCluster().startNode();
        internalCluster().validateClusterFormed();

        // assert repo gets registered
        GetRepositoriesRequest gr = new GetRepositoriesRequest(new String[] { REPOSITORY_NAME });
        GetRepositoriesResponse getRepositoriesResponse = client.admin().cluster().getRepositories(gr).actionGet();
        assertEquals(1, getRepositoriesResponse.repositories().size());

        // Index some more docs
        int currentDoc = numAutoGenDocs.get();
        int finalCurrentDoc1 = currentDoc;
        waitUntil(() -> numAutoGenDocs.get() > finalCurrentDoc1 + 5);

        // Change direction to remote store
        updateSettingsRequest.persistentSettings(Settings.builder().put(MIGRATION_DIRECTION_SETTING.getKey(), "remote_store"));
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        logger.info("-->  relocating from {} to {} ", docRepNodes, remoteNode);
        client().admin()
            .cluster()
            .prepareReroute()
            .add(new MoveAllocationCommand("test", 0, primaryNodeName("test"), remoteNode))
            .execute()
            .actionGet();
        waitForRelocation();
        assertEquals(remoteNode, primaryNodeName("test"));
        logger.info("-->  relocation from docrep to remote  complete");

        // Index some more docs
        currentDoc = numAutoGenDocs.get();
        int finalCurrentDoc = currentDoc;
        waitUntil(() -> numAutoGenDocs.get() > finalCurrentDoc + 5);

        client().admin()
            .cluster()
            .prepareReroute()
            .add(new MoveAllocationCommand("test", 0, remoteNode, remoteNode2))
            .execute()
            .actionGet();
        waitForRelocation();
        assertEquals(remoteNode2, primaryNodeName("test"));

        logger.info("-->  relocation from remote to remote  complete");

        finished.set(true);
        asyncIndexingService.stopIndexing();
        refresh("test");
        DensityAssertions.assertHitCount(
            client().prepareSearch("test").setTrackTotalHits(true).get(),
            asyncIndexingService.getIndexedDocs()
        );
        DensityAssertions.assertHitCount(
            client().prepareSearch("test")
                .setTrackTotalHits(true)// extra paranoia ;)
                .setQuery(QueryBuilders.termQuery("auto", true))
                .get(),
            asyncIndexingService.getIndexedDocs()
        );
    }

    public void testMixedModeRelocation_RemoteSeedingFail() throws Exception {
        String docRepNode = internalCluster().startNode();
        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(Settings.builder().put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), "mixed"));
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        // create shard with 0 replica and 1 shard
        client().admin().indices().prepareCreate("test").setSettings(indexSettings()).setMapping("field", "type=text").get();
        ensureGreen("test");

        AsyncIndexingService asyncIndexingService = new AsyncIndexingService("test");
        asyncIndexingService.startIndexing();

        refresh("test");

        // add remote node in mixed mode cluster
        setAddRemote(true);
        String remoteNode = internalCluster().startNode();
        internalCluster().validateClusterFormed();

        setFailRate(REPOSITORY_NAME, 100);
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(RecoverySettings.INDICES_INTERNAL_REMOTE_UPLOAD_TIMEOUT.getKey(), "10s"))
            .get();

        // Change direction to remote store
        updateSettingsRequest.persistentSettings(Settings.builder().put(MIGRATION_DIRECTION_SETTING.getKey(), "remote_store"));
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        logger.info("--> relocating from {} to {} ", docRepNode, remoteNode);
        client().admin().cluster().prepareReroute().add(new MoveAllocationCommand("test", 0, docRepNode, remoteNode)).execute().actionGet();
        ClusterHealthResponse clusterHealthResponse = client().admin()
            .cluster()
            .prepareHealth()
            .setTimeout(TimeValue.timeValueSeconds(5))
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNoRelocatingShards(true)
            .execute()
            .actionGet();

        assertTrue(clusterHealthResponse.getRelocatingShards() == 1);
        // waiting more than waitForRemoteStoreSync's sleep time of 30 sec to deterministically fail
        Thread.sleep(40000);

        ClusterHealthRequest healthRequest = Requests.clusterHealthRequest()
            .waitForNoRelocatingShards(true)
            .waitForNoInitializingShards(true);
        ClusterHealthResponse actionGet = client().admin().cluster().health(healthRequest).actionGet();
        assertEquals(actionGet.getRelocatingShards(), 0);
        assertEquals(docRepNode, primaryNodeName("test"));

        asyncIndexingService.stopIndexing();
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(RecoverySettings.INDICES_INTERNAL_REMOTE_UPLOAD_TIMEOUT.getKey(), (String) null))
            .get();
    }

    public void testMixedModeRelocation_FailInFinalize() throws Exception {
        String docRepNode = internalCluster().startNode();
        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(Settings.builder().put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), "mixed"));
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        // create shard with 0 replica and 1 shard
        client().admin().indices().prepareCreate("test").setSettings(indexSettings()).setMapping("field", "type=text").get();
        ensureGreen("test");

        AsyncIndexingService asyncIndexingService = new AsyncIndexingService("test");
        asyncIndexingService.startIndexing();

        refresh("test");

        // add remote node in mixed mode cluster
        setAddRemote(true);
        String remoteNode = internalCluster().startNode();
        internalCluster().validateClusterFormed();

        AtomicBoolean failFinalize = new AtomicBoolean(true);

        MockTransportService remoteNodeTransportService = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            remoteNode
        );

        remoteNodeTransportService.addRequestHandlingBehavior(
            PeerRecoveryTargetService.Actions.FINALIZE,
            (handler, request, channel, task) -> {
                if (failFinalize.get()) {
                    throw new IOException("Failing finalize");
                } else {
                    handler.messageReceived(request, channel, task);
                }
            }
        );

        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(RecoverySettings.INDICES_INTERNAL_REMOTE_UPLOAD_TIMEOUT.getKey(), "40s"))
            .get();

        // Change direction to remote store
        updateSettingsRequest.persistentSettings(Settings.builder().put(MIGRATION_DIRECTION_SETTING.getKey(), "remote_store"));
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        logger.info("--> relocating from {} to {} ", docRepNode, remoteNode);
        client().admin().cluster().prepareReroute().add(new MoveAllocationCommand("test", 0, docRepNode, remoteNode)).execute().actionGet();
        ClusterHealthResponse clusterHealthResponse = client().admin()
            .cluster()
            .prepareHealth()
            .setTimeout(TimeValue.timeValueSeconds(5))
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNoRelocatingShards(true)
            .execute()
            .actionGet();

        assertTrue(clusterHealthResponse.getRelocatingShards() == 1);

        ClusterHealthRequest healthRequest = Requests.clusterHealthRequest()
            .waitForNoRelocatingShards(true)
            .waitForNoInitializingShards(true);
        ClusterHealthResponse actionGet = client().admin().cluster().health(healthRequest).actionGet();
        assertEquals(actionGet.getRelocatingShards(), 0);
        assertEquals(docRepNode, primaryNodeName("test"));

        // now unblock it
        logger.info("Unblocking the finalize recovery now");
        failFinalize.set(false);

        client().admin().cluster().prepareReroute().add(new MoveAllocationCommand("test", 0, docRepNode, remoteNode)).execute().actionGet();
        waitForRelocation(TimeValue.timeValueSeconds(90));

        asyncIndexingService.stopIndexing();
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(RecoverySettings.INDICES_INTERNAL_REMOTE_UPLOAD_TIMEOUT.getKey(), (String) null))
            .get();
    }
}
