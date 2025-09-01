/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.index.remote;

import org.density.cluster.service.ClusterService;
import org.density.common.settings.ClusterSettings;
import org.density.common.settings.Settings;
import org.density.core.index.shard.ShardId;
import org.density.index.shard.IndexShard;
import org.density.test.ClusterServiceUtils;
import org.density.test.DensityTestCase;
import org.density.threadpool.TestThreadPool;
import org.density.threadpool.ThreadPool;

import static org.density.index.remote.RemoteStoreTestsHelper.createIndexShard;

public class RemoteStoreStatsTrackerFactoryTests extends DensityTestCase {
    private ThreadPool threadPool;
    private ClusterService clusterService;
    private Settings settings;
    private ShardId shardId;
    private IndexShard indexShard;
    private RemoteStoreStatsTrackerFactory remoteStoreStatsTrackerFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        shardId = new ShardId("index", "uuid", 0);
        indexShard = createIndexShard(shardId, true);
        threadPool = new TestThreadPool(getTestName());
        settings = Settings.builder()
            .put(
                RemoteStoreStatsTrackerFactory.MOVING_AVERAGE_WINDOW_SIZE.getKey(),
                RemoteStoreStatsTrackerFactory.Defaults.MOVING_AVERAGE_WINDOW_SIZE_MIN_VALUE
            )
            .build();
        clusterService = ClusterServiceUtils.createClusterService(
            settings,
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            threadPool
        );
        remoteStoreStatsTrackerFactory = new RemoteStoreStatsTrackerFactory(clusterService, settings);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdownNow();
    }

    public void testAfterIndexShardCreatedForRemoteBackedIndex() {
        remoteStoreStatsTrackerFactory.afterIndexShardCreated(indexShard);
        assertNotNull(remoteStoreStatsTrackerFactory.getRemoteSegmentTransferTracker(indexShard.shardId()));
    }

    public void testAfterIndexShardCreatedForNonRemoteBackedIndex() {
        indexShard = createIndexShard(shardId, false);
        remoteStoreStatsTrackerFactory.afterIndexShardCreated(indexShard);
        assertNull(remoteStoreStatsTrackerFactory.getRemoteSegmentTransferTracker(indexShard.shardId()));
    }

    public void testAfterIndexShardClosed() {
        remoteStoreStatsTrackerFactory.afterIndexShardCreated(indexShard);
        assertNotNull(remoteStoreStatsTrackerFactory.getRemoteSegmentTransferTracker(shardId));
        remoteStoreStatsTrackerFactory.afterIndexShardClosed(shardId, indexShard, indexShard.indexSettings().getSettings());
        assertNull(remoteStoreStatsTrackerFactory.getRemoteSegmentTransferTracker(shardId));
    }

    public void testGetConfiguredSettings() {
        assertEquals(
            RemoteStoreStatsTrackerFactory.Defaults.MOVING_AVERAGE_WINDOW_SIZE_MIN_VALUE,
            remoteStoreStatsTrackerFactory.getMovingAverageWindowSize()
        );
    }

    public void testInvalidMovingAverageWindowSize() {
        Settings settings = Settings.builder()
            .put(
                RemoteStoreStatsTrackerFactory.MOVING_AVERAGE_WINDOW_SIZE.getKey(),
                RemoteStoreStatsTrackerFactory.Defaults.MOVING_AVERAGE_WINDOW_SIZE_MIN_VALUE - 1
            )
            .build();
        assertThrows(
            "Failed to parse value",
            IllegalArgumentException.class,
            () -> new RemoteStoreStatsTrackerFactory(
                ClusterServiceUtils.createClusterService(
                    settings,
                    new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                    threadPool
                ),
                settings
            )
        );
    }

    public void testUpdateAfterGetConfiguredSettings() {
        assertEquals(
            RemoteStoreStatsTrackerFactory.Defaults.MOVING_AVERAGE_WINDOW_SIZE_MIN_VALUE,
            remoteStoreStatsTrackerFactory.getMovingAverageWindowSize()
        );

        Settings newSettings = Settings.builder().put(RemoteStoreStatsTrackerFactory.MOVING_AVERAGE_WINDOW_SIZE.getKey(), 102).build();

        clusterService.getClusterSettings().applySettings(newSettings);

        // Check moving average window size updated
        assertEquals(102, remoteStoreStatsTrackerFactory.getMovingAverageWindowSize());
    }

    public void testGetDefaultSettings() {
        remoteStoreStatsTrackerFactory = new RemoteStoreStatsTrackerFactory(
            ClusterServiceUtils.createClusterService(
                Settings.EMPTY,
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                threadPool
            ),
            Settings.EMPTY
        );
        // Check moving average window size updated
        assertEquals(
            RemoteStoreStatsTrackerFactory.Defaults.MOVING_AVERAGE_WINDOW_SIZE,
            remoteStoreStatsTrackerFactory.getMovingAverageWindowSize()
        );
    }
}
