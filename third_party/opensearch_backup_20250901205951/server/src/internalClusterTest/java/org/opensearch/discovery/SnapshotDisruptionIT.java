/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright Density Contributors. See
 * GitHub history for details.
 */

package org.density.discovery;

import org.density.ExceptionsHelper;
import org.density.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.density.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.density.action.index.IndexRequestBuilder;
import org.density.cluster.ClusterChangedEvent;
import org.density.cluster.ClusterStateListener;
import org.density.cluster.SnapshotsInProgress;
import org.density.cluster.metadata.RepositoriesMetadata;
import org.density.cluster.metadata.RepositoryMetadata;
import org.density.cluster.service.ClusterService;
import org.density.common.action.ActionFuture;
import org.density.common.settings.Settings;
import org.density.common.unit.TimeValue;
import org.density.common.xcontent.json.JsonXContent;
import org.density.plugins.Plugin;
import org.density.snapshots.AbstractSnapshotIntegTestCase;
import org.density.snapshots.SnapshotException;
import org.density.snapshots.SnapshotInfo;
import org.density.snapshots.SnapshotMissingException;
import org.density.snapshots.SnapshotState;
import org.density.snapshots.mockstore.MockRepository;
import org.density.test.DensityIntegTestCase;
import org.density.test.disruption.NetworkDisruption;
import org.density.test.transport.MockTransportService;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.density.test.hamcrest.DensityAssertions.assertAcked;
import static org.density.test.hamcrest.DensityAssertions.assertFutureThrows;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;

/**
 * Tests snapshot operations during disruptions.
 */
@DensityIntegTestCase.ClusterScope(scope = DensityIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SnapshotDisruptionIT extends AbstractSnapshotIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class, MockRepository.Plugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).put(AbstractDisruptionTestCase.DEFAULT_SETTINGS).build();
    }

    public void testDisruptionAfterFinalization() throws Exception {
        final String idxName = "test";
        internalCluster().startClusterManagerOnlyNodes(3);
        final String dataNode = internalCluster().startDataOnlyNode();
        ensureStableCluster(4);

        createRandomIndex(idxName);

        createRepository("test-repo", "fs");

        final String clusterManagerNode1 = internalCluster().getClusterManagerName();

        NetworkDisruption networkDisruption = isolateClusterManagerDisruption(NetworkDisruption.UNRESPONSIVE);
        internalCluster().setDisruptionScheme(networkDisruption);

        ClusterService clusterService = internalCluster().clusterService(clusterManagerNode1);
        CountDownLatch disruptionStarted = new CountDownLatch(1);
        clusterService.addListener(new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                SnapshotsInProgress snapshots = event.state().custom(SnapshotsInProgress.TYPE);
                if (snapshots != null && snapshots.entries().size() > 0) {
                    final SnapshotsInProgress.Entry snapshotEntry = snapshots.entries().get(0);
                    if (snapshotEntry.state() == SnapshotsInProgress.State.SUCCESS) {
                        final RepositoriesMetadata repoMeta = event.state().metadata().custom(RepositoriesMetadata.TYPE);
                        final RepositoryMetadata metadata = repoMeta.repository("test-repo");
                        if (metadata.pendingGeneration() > snapshotEntry.repositoryStateId()) {
                            logger.info("--> starting disruption");
                            networkDisruption.startDisrupting();
                            clusterService.removeListener(this);
                            disruptionStarted.countDown();
                        }
                    }
                }
            }
        });

        final String snapshot = "test-snap";

        logger.info("--> starting snapshot");
        ActionFuture<CreateSnapshotResponse> future = client(clusterManagerNode1).admin()
            .cluster()
            .prepareCreateSnapshot("test-repo", snapshot)
            .setWaitForCompletion(true)
            .setIndices(idxName)
            .execute();

        logger.info("--> waiting for disruption to start");
        assertTrue(disruptionStarted.await(1, TimeUnit.MINUTES));

        awaitNoMoreRunningOperations(dataNode);

        logger.info("--> verify that snapshot was successful or no longer exist");
        assertBusy(() -> {
            try {
                assertSnapshotExists("test-repo", snapshot);
            } catch (SnapshotMissingException exception) {
                logger.info("--> done verifying, snapshot doesn't exist");
            }
        }, 1, TimeUnit.MINUTES);

        logger.info("--> stopping disrupting");
        networkDisruption.stopDisrupting();
        ensureStableCluster(4, clusterManagerNode1);
        logger.info("--> done");

        try {
            future.get();
            fail("Should have failed because the node disconnected from the cluster during snapshot finalization");
        } catch (Exception ex) {
            final SnapshotException sne = (SnapshotException) ExceptionsHelper.unwrap(ex, SnapshotException.class);
            assertNotNull(sne);
            assertThat(
                sne.getMessage(),
                either(endsWith(" Failed to update cluster state during snapshot finalization")).or(endsWith(" no longer cluster-manager"))
            );
            assertThat(sne.getSnapshotName(), is(snapshot));
        }

        awaitNoMoreRunningOperations(dataNode);
    }

    public void testDisruptionAfterShardFinalization() throws Exception {
        final String idxName = "test";
        internalCluster().startClusterManagerOnlyNodes(1);
        internalCluster().startDataOnlyNode();
        ensureStableCluster(2);
        createIndex(idxName);
        index(idxName, "type", JsonXContent.contentBuilder().startObject().field("foo", "bar").endObject());

        final String repoName = "test-repo";
        createRepository(repoName, "mock");

        final String clusterManagerNode = internalCluster().getClusterManagerName();

        blockAllDataNodes(repoName);

        final String snapshot = "test-snap";
        logger.info("--> starting snapshot");
        ActionFuture<CreateSnapshotResponse> future = client(clusterManagerNode).admin()
            .cluster()
            .prepareCreateSnapshot(repoName, snapshot)
            .setWaitForCompletion(true)
            .execute();

        waitForBlockOnAnyDataNode(repoName, TimeValue.timeValueSeconds(10L));

        NetworkDisruption networkDisruption = isolateClusterManagerDisruption(NetworkDisruption.DISCONNECT);
        internalCluster().setDisruptionScheme(networkDisruption);
        networkDisruption.startDisrupting();

        final CreateSnapshotResponse createSnapshotResponse = future.get();
        final SnapshotInfo snapshotInfo = createSnapshotResponse.getSnapshotInfo();
        assertThat(snapshotInfo.state(), is(SnapshotState.PARTIAL));

        logger.info("--> stopping disrupting");
        networkDisruption.stopDisrupting();
        unblockAllDataNodes(repoName);

        ensureStableCluster(2, clusterManagerNode);
        logger.info("--> done");

        logger.info("--> recreate the index with potentially different shard counts");
        client().admin().indices().prepareDelete(idxName).get();
        createIndex(idxName);
        index(idxName, "type", JsonXContent.contentBuilder().startObject().field("foo", "bar").endObject());

        logger.info("--> run a snapshot that fails to finalize but succeeds on the data node");
        blockClusterManagerFromFinalizingSnapshotOnIndexFile(repoName);
        final ActionFuture<CreateSnapshotResponse> snapshotFuture = client(clusterManagerNode).admin()
            .cluster()
            .prepareCreateSnapshot(repoName, "snapshot-2")
            .setWaitForCompletion(true)
            .execute();
        waitForBlock(clusterManagerNode, repoName, TimeValue.timeValueSeconds(10L));
        unblockNode(repoName, clusterManagerNode);
        assertFutureThrows(snapshotFuture, SnapshotException.class);

        logger.info("--> create a snapshot expected to be successful");
        final CreateSnapshotResponse successfulSnapshot = client(clusterManagerNode).admin()
            .cluster()
            .prepareCreateSnapshot(repoName, "snapshot-2")
            .setWaitForCompletion(true)
            .get();
        final SnapshotInfo successfulSnapshotInfo = successfulSnapshot.getSnapshotInfo();
        assertThat(successfulSnapshotInfo.state(), is(SnapshotState.SUCCESS));

        logger.info("--> making sure snapshot delete works out cleanly");
        assertAcked(client().admin().cluster().prepareDeleteSnapshot(repoName, "snapshot-2").get());
    }

    public void testClusterManagerFailOverDuringShardSnapshots() throws Exception {
        internalCluster().startClusterManagerOnlyNodes(3);
        final String dataNode = internalCluster().startDataOnlyNode();
        ensureStableCluster(4);
        final String repoName = "test-repo";
        createRepository(repoName, "mock");

        final String indexName = "index-one";
        createIndex(indexName);
        client().prepareIndex(indexName).setSource("foo", "bar").get();

        blockDataNode(repoName, dataNode);

        logger.info("--> create snapshot via cluster-manager node client");
        final ActionFuture<CreateSnapshotResponse> snapshotResponse = internalCluster().clusterManagerClient()
            .admin()
            .cluster()
            .prepareCreateSnapshot(repoName, "test-snap")
            .setWaitForCompletion(true)
            .execute();

        waitForBlock(dataNode, repoName, TimeValue.timeValueSeconds(30L));

        final NetworkDisruption networkDisruption = isolateClusterManagerDisruption(NetworkDisruption.DISCONNECT);
        internalCluster().setDisruptionScheme(networkDisruption);
        networkDisruption.startDisrupting();
        ensureStableCluster(3, dataNode);
        unblockNode(repoName, dataNode);

        networkDisruption.stopDisrupting();
        awaitNoMoreRunningOperations(dataNode);

        logger.info("--> make sure isolated cluster-manager responds to snapshot request");
        final SnapshotException sne = expectThrows(
            SnapshotException.class,
            () -> snapshotResponse.actionGet(TimeValue.timeValueSeconds(30L))
        );
        assertThat(sne.getMessage(), endsWith("no longer cluster-manager"));
    }

    private void assertSnapshotExists(String repository, String snapshot) {
        GetSnapshotsResponse snapshotsStatusResponse = dataNodeClient().admin()
            .cluster()
            .prepareGetSnapshots(repository)
            .setSnapshots(snapshot)
            .get();
        SnapshotInfo snapshotInfo = snapshotsStatusResponse.getSnapshots().get(0);
        assertEquals(SnapshotState.SUCCESS, snapshotInfo.state());
        assertEquals(snapshotInfo.totalShards(), snapshotInfo.successfulShards());
        assertEquals(0, snapshotInfo.failedShards());
        logger.info("--> done verifying, snapshot exists");
    }

    private void createRandomIndex(String idxName) throws InterruptedException {
        assertAcked(prepareCreate(idxName, 0, indexSettingsNoReplicas(between(1, 5))));
        logger.info("--> indexing some data");
        final int numdocs = randomIntBetween(10, 100);
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numdocs];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = client().prepareIndex(idxName).setId(Integer.toString(i)).setSource("field1", "bar " + i);
        }
        indexRandom(true, builders);
    }
}
