/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.remotemigration;

import org.density.action.admin.indices.stats.ShardStats;
import org.density.cluster.metadata.IndexMetadata;
import org.density.cluster.node.DiscoveryNode;
import org.density.cluster.node.DiscoveryNodes;
import org.density.cluster.routing.ShardRouting;
import org.density.common.blobstore.BlobPath;
import org.density.common.settings.Settings;
import org.density.common.unit.TimeValue;
import org.density.core.util.FileSystemUtils;
import org.density.index.remote.RemoteSegmentStats;
import org.density.index.translog.RemoteTranslogStats;
import org.density.indices.RemoteStoreSettings;
import org.density.test.InternalTestCluster;
import org.density.test.DensityIntegTestCase;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.density.index.remote.RemoteStoreEnums.DataCategory.SEGMENTS;
import static org.density.index.remote.RemoteStoreEnums.DataType.DATA;
import static org.density.index.store.RemoteSegmentStoreDirectory.SEGMENT_NAME_UUID_SEPARATOR;

@DensityIntegTestCase.ClusterScope(scope = DensityIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemotePrimaryLocalRecoveryIT extends MigrationBaseTestCase {
    String indexName = "idx1";
    int numOfNodes = randomIntBetween(6, 9);

    /**
     * Tests local recovery sanity in the happy path flow
     */
    public void testLocalRecoveryRollingRestart() throws Exception {
        triggerRollingRestartForRemoteMigration(0);
        internalCluster().stopAllNodes();
    }

    /**
     * Tests local recovery sanity during remote migration with a node restart in between
     */
    public void testLocalRecoveryRollingRestartAndNodeFailure() throws Exception {
        triggerRollingRestartForRemoteMigration(0);

        DiscoveryNodes discoveryNodes = internalCluster().client().admin().cluster().prepareState().get().getState().getNodes();
        DiscoveryNode nodeToRestart = (DiscoveryNode) discoveryNodes.getDataNodes().values().toArray()[randomIntBetween(0, numOfNodes - 4)];
        internalCluster().restartNode(nodeToRestart.getName());

        Map<ShardRouting, ShardStats> shardStatsMap = internalCluster().client().admin().indices().prepareStats(indexName).get().asMap();
        for (Map.Entry<ShardRouting, ShardStats> entry : shardStatsMap.entrySet()) {
            ShardRouting shardRouting = entry.getKey();
            ShardStats shardStats = entry.getValue();
            if (nodeToRestart.equals(shardRouting.currentNodeId())) {
                RemoteSegmentStats remoteSegmentStats = shardStats.getStats().getSegments().getRemoteSegmentStats();
                assertTrue(remoteSegmentStats.getTotalUploadTime() > 0);
                assertTrue(remoteSegmentStats.getUploadBytesSucceeded() > 0);
            }

            String segmentsPathFixedPrefix = RemoteStoreSettings.CLUSTER_REMOTE_STORE_SEGMENTS_PATH_PREFIX.get(getNodeSettings());
            assertBusy(() -> {
                String shardPath = getShardLevelBlobPath(
                    client(),
                    indexName,
                    new BlobPath(),
                    String.valueOf(shardRouting.getId()),
                    SEGMENTS,
                    DATA,
                    segmentsPathFixedPrefix
                ).buildAsString();
                Path segmentDataRepoPath = segmentRepoPath.resolve(shardPath);
                List<String> segmentsNFilesInRepo = Arrays.stream(FileSystemUtils.files(segmentDataRepoPath))
                    .filter(path -> path.getFileName().toString().contains("segments_"))
                    .map(path -> path.getFileName().toString())
                    .collect(Collectors.toList());
                Set<String> expectedUniqueSegmentsNFiles = segmentsNFilesInRepo.stream()
                    .map(fileName -> fileName.split(SEGMENT_NAME_UUID_SEPARATOR)[0])
                    .collect(Collectors.toSet());
                assertEquals(
                    "Expected no duplicate segments_N files in remote but duplicates were found " + segmentsNFilesInRepo,
                    expectedUniqueSegmentsNFiles.size(),
                    segmentsNFilesInRepo.size()
                );
            }, 90, TimeUnit.SECONDS);
        }

        internalCluster().stopAllNodes();
    }

    /**
     * Tests local recovery flow sanity in the happy path flow with replicas in place
     */
    public void testLocalRecoveryFlowWithReplicas() throws Exception {
        triggerRollingRestartForRemoteMigration(randomIntBetween(1, 2));
        internalCluster().stopAllNodes();
    }

    /**
     * Helper method to run a rolling restart for migration to remote backed cluster
     */
    private void triggerRollingRestartForRemoteMigration(int replicaCount) throws Exception {
        internalCluster().startClusterManagerOnlyNodes(3);
        internalCluster().startNodes(numOfNodes - 3);

        // create index
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, replicaCount)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 10))
            .build();
        createIndex(indexName, indexSettings);
        ensureGreen(indexName);
        indexBulk(indexName, randomIntBetween(100, 10000));
        refresh(indexName);
        indexBulk(indexName, randomIntBetween(100, 10000));

        initDocRepToRemoteMigration();

        // rolling restart
        final Settings remoteNodeAttributes = remoteStoreClusterSettings(
            REPOSITORY_NAME,
            segmentRepoPath,
            REPOSITORY_2_NAME,
            translogRepoPath
        );
        internalCluster().rollingRestart(new InternalTestCluster.RestartCallback() {
            // Update remote attributes
            @Override
            public Settings onNodeStopped(String nodeName) {
                return remoteNodeAttributes;
            }
        });
        ensureStableCluster(numOfNodes);
        ensureGreen(TimeValue.timeValueSeconds(90), indexName);
        assertEquals(internalCluster().size(), numOfNodes);

        // Assert on remote uploads
        Map<ShardRouting, ShardStats> shardStatsMap = internalCluster().client().admin().indices().prepareStats(indexName).get().asMap();
        DiscoveryNodes discoveryNodes = internalCluster().client().admin().cluster().prepareState().get().getState().getNodes();
        shardStatsMap.forEach((shardRouting, shardStats) -> {
            if (discoveryNodes.get(shardRouting.currentNodeId()).isRemoteStoreNode() && shardRouting.primary()) {
                RemoteSegmentStats remoteSegmentStats = shardStats.getStats().getSegments().getRemoteSegmentStats();
                assertTrue(remoteSegmentStats.getTotalUploadTime() > 0);
                assertTrue(remoteSegmentStats.getUploadBytesSucceeded() > 0);
            }
        });

        // Assert on new remote uploads after seeding
        indexBulk(indexName, randomIntBetween(100, 10000));
        refresh(indexName);
        indexBulk(indexName, randomIntBetween(100, 10000));
        Map<ShardRouting, ShardStats> newShardStatsMap = internalCluster().client().admin().indices().prepareStats(indexName).get().asMap();
        newShardStatsMap.forEach((shardRouting, shardStats) -> {
            if (discoveryNodes.get(shardRouting.currentNodeId()).isRemoteStoreNode() && shardRouting.primary()) {
                RemoteSegmentStats prevRemoteSegmentStats = shardStatsMap.get(shardRouting)
                    .getStats()
                    .getSegments()
                    .getRemoteSegmentStats();
                RemoteSegmentStats newRemoteSegmentStats = shardStats.getStats().getSegments().getRemoteSegmentStats();
                assertTrue(newRemoteSegmentStats.getTotalUploadTime() > prevRemoteSegmentStats.getTotalUploadTime());
                assertTrue(newRemoteSegmentStats.getUploadBytesSucceeded() > prevRemoteSegmentStats.getUploadBytesSucceeded());

                RemoteTranslogStats prevRemoteTranslogStats = shardStatsMap.get(shardRouting)
                    .getStats()
                    .getTranslog()
                    .getRemoteTranslogStats();
                RemoteTranslogStats newRemoteTranslogStats = shardStats.getStats().getTranslog().getRemoteTranslogStats();
                assertTrue(newRemoteTranslogStats.getUploadBytesSucceeded() > prevRemoteTranslogStats.getUploadBytesSucceeded());
            }
        });
    }
}
