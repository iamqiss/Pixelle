/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.node.resource.tracker;

import org.density.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.density.common.settings.ClusterSettings;
import org.density.common.settings.Settings;
import org.density.common.unit.TimeValue;
import org.density.monitor.fs.FsService;
import org.density.test.DensitySingleNodeTestCase;
import org.density.threadpool.TestThreadPool;
import org.density.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.TimeUnit;

import static org.density.test.hamcrest.DensityAssertions.assertAcked;
import static org.hamcrest.Matchers.greaterThan;
import static org.mockito.Mockito.mock;

/**
 * Tests to assert resource usage trackers retrieving resource utilization averages
 */
public class NodeResourceUsageTrackerTests extends DensitySingleNodeTestCase {
    ThreadPool threadPool;

    @Before
    public void setup() {
        threadPool = new TestThreadPool(getClass().getName());
    }

    @After
    public void cleanup() {
        ThreadPool.terminate(threadPool, 5, TimeUnit.SECONDS);
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().putNull("*"))
                .setTransientSettings(Settings.builder().putNull("*"))
        );
    }

    public void testStats() throws Exception {
        Settings settings = Settings.builder()
            .put(ResourceTrackerSettings.GLOBAL_JVM_USAGE_AC_WINDOW_DURATION_SETTING.getKey(), new TimeValue(500, TimeUnit.MILLISECONDS))
            .build();
        NodeResourceUsageTracker tracker = new NodeResourceUsageTracker(
            mock(FsService.class),
            threadPool,
            settings,
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        tracker.start();
        /**
         * Asserting memory utilization to be greater than 0
         * cpu percent used is mostly 0, so skipping assertion for that
         */
        assertBusy(() -> assertThat(tracker.getMemoryUtilizationPercent(), greaterThan(0.0)), 5, TimeUnit.SECONDS);
        tracker.stop();
        tracker.close();
    }

    public void testUpdateSettings() {
        NodeResourceUsageTracker tracker = new NodeResourceUsageTracker(
            mock(FsService.class),
            threadPool,
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );

        assertEquals(tracker.getResourceTrackerSettings().getCpuWindowDuration().getSeconds(), 30);
        assertEquals(tracker.getResourceTrackerSettings().getMemoryWindowDuration().getSeconds(), 30);
        assertEquals(tracker.getResourceTrackerSettings().getIoWindowDuration().getSeconds(), 120);

        Settings settings = Settings.builder()
            .put(ResourceTrackerSettings.GLOBAL_CPU_USAGE_AC_WINDOW_DURATION_SETTING.getKey(), "10s")
            .build();
        ClusterUpdateSettingsResponse response = client().admin().cluster().prepareUpdateSettings().setPersistentSettings(settings).get();
        assertEquals(
            "10s",
            response.getPersistentSettings().get(ResourceTrackerSettings.GLOBAL_CPU_USAGE_AC_WINDOW_DURATION_SETTING.getKey())
        );

        Settings jvmsettings = Settings.builder()
            .put(ResourceTrackerSettings.GLOBAL_JVM_USAGE_AC_WINDOW_DURATION_SETTING.getKey(), "5s")
            .build();
        response = client().admin().cluster().prepareUpdateSettings().setPersistentSettings(jvmsettings).get();
        assertEquals(
            "5s",
            response.getPersistentSettings().get(ResourceTrackerSettings.GLOBAL_JVM_USAGE_AC_WINDOW_DURATION_SETTING.getKey())
        );
        Settings ioSettings = Settings.builder()
            .put(ResourceTrackerSettings.GLOBAL_IO_USAGE_AC_WINDOW_DURATION_SETTING.getKey(), "20s")
            .build();
        response = client().admin().cluster().prepareUpdateSettings().setPersistentSettings(ioSettings).get();
        assertEquals(
            "20s",
            response.getPersistentSettings().get(ResourceTrackerSettings.GLOBAL_IO_USAGE_AC_WINDOW_DURATION_SETTING.getKey())
        );
    }
}
