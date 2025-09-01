/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.remotestore;

import org.density.action.admin.cluster.health.ClusterHealthResponse;
import org.density.action.admin.cluster.stats.ClusterStatsResponse;
import org.density.cluster.metadata.IndexMetadata;
import org.density.cluster.routing.allocation.command.MoveAllocationCommand;
import org.density.common.Priority;
import org.density.common.settings.Settings;
import org.density.common.unit.TimeValue;
import org.density.core.index.Index;
import org.density.index.IndexService;
import org.density.index.ReplicationStats;
import org.density.index.shard.IndexShard;
import org.density.indices.IndicesService;
import org.density.indices.replication.SegmentReplicationState;
import org.density.indices.replication.SegmentReplicationTarget;
import org.density.indices.replication.SegmentReplicationTargetService;
import org.density.test.DensityIntegTestCase;
import org.density.test.disruption.SlowClusterStateProcessing;

import java.nio.file.Path;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.density.test.hamcrest.DensityAssertions.assertAcked;

/**
 * This class runs tests with remote store + segRep while blocking file downloads
 */
@DensityIntegTestCase.ClusterScope(scope = DensityIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SegmentReplicationUsingRemoteStoreDisruptionIT extends AbstractRemoteStoreMockRepositoryIntegTestCase {

    @Override
    public Settings indexSettings() {
        return remoteStoreIndexSettings(1);
    }

    public void testCancelReplicationWhileSyncingSegments() throws Exception {
        Path location = randomRepoPath().toAbsolutePath();
        setup(location, 0d, "metadata", Long.MAX_VALUE, 1);

        final Set<String> dataNodeNames = internalCluster().getDataNodeNames();
        final String replicaNode = getNode(dataNodeNames, false);
        final String primaryNode = getNode(dataNodeNames, true);

        SegmentReplicationTargetService targetService = internalCluster().getInstance(SegmentReplicationTargetService.class, replicaNode);
        ensureGreen(INDEX_NAME);
        blockNodeOnAnySegmentFile(REPOSITORY_NAME, replicaNode);
        final IndexShard indexShard = getIndexShard(replicaNode, INDEX_NAME);
        indexSingleDoc();
        refresh(INDEX_NAME);
        waitForBlock(replicaNode, REPOSITORY_NAME, TimeValue.timeValueSeconds(10));
        SegmentReplicationTarget segmentReplicationTarget = targetService.get(indexShard.shardId());
        assertNotNull(segmentReplicationTarget);
        assertEquals(SegmentReplicationState.Stage.GET_FILES, segmentReplicationTarget.state().getStage());
        assertTrue(segmentReplicationTarget.refCount() > 0);
        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings(INDEX_NAME)
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0))
        );
        assertNull(targetService.getOngoingEventSegmentReplicationState(indexShard.shardId()));
        assertEquals("Target should be closed", 0, segmentReplicationTarget.refCount());
        unblockNode(REPOSITORY_NAME, replicaNode);
        cleanupRepo();
    }

    public void testCancelReplicationWhileFetchingMetadata() throws Exception {
        Path location = randomRepoPath().toAbsolutePath();
        setup(location, 0d, "metadata", Long.MAX_VALUE, 1);

        final Set<String> dataNodeNames = internalCluster().getDataNodeNames();
        final String replicaNode = getNode(dataNodeNames, false);

        SegmentReplicationTargetService targetService = internalCluster().getInstance(SegmentReplicationTargetService.class, replicaNode);
        ensureGreen(INDEX_NAME);
        blockNodeOnAnyFiles(REPOSITORY_NAME, replicaNode);
        final IndexShard indexShard = getIndexShard(replicaNode, INDEX_NAME);
        indexSingleDoc();
        refresh(INDEX_NAME);
        waitForBlock(replicaNode, REPOSITORY_NAME, TimeValue.timeValueSeconds(10));
        SegmentReplicationTarget segmentReplicationTarget = targetService.get(indexShard.shardId());
        assertNotNull(segmentReplicationTarget);
        assertEquals(SegmentReplicationState.Stage.GET_CHECKPOINT_INFO, segmentReplicationTarget.state().getStage());
        assertTrue(segmentReplicationTarget.refCount() > 0);
        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings(INDEX_NAME)
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0))
        );
        assertNull(targetService.get(indexShard.shardId()));
        assertEquals("Target should be closed", 0, segmentReplicationTarget.refCount());
        unblockNode(REPOSITORY_NAME, replicaNode);
        cleanupRepo();
    }

    public void testUpdateVisibleCheckpointWithLaggingClusterStateUpdates_primaryRelocation() throws Exception {
        Path location = randomRepoPath().toAbsolutePath();
        Settings nodeSettings = Settings.builder().put(buildRemoteStoreNodeAttributes(location, 0d, "metadata", Long.MAX_VALUE)).build();
        internalCluster().startClusterManagerOnlyNode(nodeSettings);
        internalCluster().startDataOnlyNodes(2, nodeSettings);
        final Settings indexSettings = Settings.builder().put(indexSettings()).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1).build();
        createIndex(INDEX_NAME, indexSettings);
        ensureGreen(INDEX_NAME);
        final Set<String> dataNodeNames = internalCluster().getDataNodeNames();
        final String replicaNode = getNode(dataNodeNames, false);
        final String oldPrimary = getNode(dataNodeNames, true);

        // index a doc.
        client().prepareIndex(INDEX_NAME).setId("1").setSource("foo", randomInt()).get();
        refresh(INDEX_NAME);

        logger.info("--> start another node");
        final String newPrimary = internalCluster().startDataOnlyNode(nodeSettings);
        ClusterHealthResponse clusterHealthResponse = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNodes("4")
            .get();
        assertEquals(clusterHealthResponse.isTimedOut(), false);

        SlowClusterStateProcessing disruption = new SlowClusterStateProcessing(replicaNode, random(), 0, 0, 1000, 2000);
        internalCluster().setDisruptionScheme(disruption);
        disruption.startDisrupting();

        // relocate the primary
        logger.info("--> relocate the shard");
        client().admin()
            .cluster()
            .prepareReroute()
            .add(new MoveAllocationCommand(INDEX_NAME, 0, oldPrimary, newPrimary))
            .execute()
            .actionGet();
        clusterHealthResponse = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNoRelocatingShards(true)
            .setTimeout(new TimeValue(5, TimeUnit.MINUTES))
            .execute()
            .actionGet();
        assertEquals(clusterHealthResponse.isTimedOut(), false);

        IndexShard newPrimary_shard = getIndexShard(newPrimary, INDEX_NAME);
        IndexShard replica = getIndexShard(replicaNode, INDEX_NAME);
        assertBusy(() -> {
            assertEquals(
                newPrimary_shard.getLatestReplicationCheckpoint().getSegmentInfosVersion(),
                replica.getLatestReplicationCheckpoint().getSegmentInfosVersion()
            );
        });

        assertBusy(() -> {
            ClusterStatsResponse clusterStatsResponse = client().admin().cluster().prepareClusterStats().get();
            ReplicationStats replicationStats = clusterStatsResponse.getIndicesStats().getSegments().getReplicationStats();
            assertEquals(0L, replicationStats.maxBytesBehind);
            assertEquals(0L, replicationStats.maxReplicationLag);
            assertEquals(0L, replicationStats.totalBytesBehind);
        });
        disruption.stopDisrupting();
        disableRepoConsistencyCheck("Remote Store Creates System Repository");
        cleanupRepo();
    }

    private String getNode(Set<String> dataNodeNames, boolean primary) {
        assertEquals(2, dataNodeNames.size());
        for (String name : dataNodeNames) {
            final IndexShard indexShard = getIndexShard(name, INDEX_NAME);
            if (indexShard.routingEntry().primary() == primary) {
                return name;
            }
        }
        return null;
    }

    private IndexShard getIndexShard(String node, String indexName) {
        final Index index = resolveIndex(indexName);
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, node);
        IndexService indexService = indicesService.indexService(index);
        assertNotNull(indexService);
        final Optional<Integer> shardId = indexService.shardIds().stream().findFirst();
        return shardId.map(indexService::getShard).orElse(null);
    }
}
