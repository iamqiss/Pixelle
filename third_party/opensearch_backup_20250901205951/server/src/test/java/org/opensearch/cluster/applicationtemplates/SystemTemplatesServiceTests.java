/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.cluster.applicationtemplates;

import org.density.cluster.service.applicationtemplates.TestSystemTemplatesRepositoryPlugin;
import org.density.common.settings.ClusterSettings;
import org.density.common.settings.Settings;
import org.density.common.util.concurrent.DensityExecutors;
import org.density.test.DensityTestCase;
import org.density.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.mockito.Mockito;

import static org.density.common.settings.ClusterSettings.BUILT_IN_CLUSTER_SETTINGS;
import static org.density.common.util.FeatureFlags.APPLICATION_BASED_CONFIGURATION_TEMPLATES;
import static org.mockito.Mockito.when;

public class SystemTemplatesServiceTests extends DensityTestCase {

    private SystemTemplatesService systemTemplatesService;

    @LockFeatureFlag(APPLICATION_BASED_CONFIGURATION_TEMPLATES)
    public void testSystemTemplatesLoaded() throws IOException {
        setupService(true);

        // First time load should happen, second time should short circuit.
        for (int iter = 1; iter <= 2; iter++) {
            systemTemplatesService.onClusterManager();
            SystemTemplatesService.Stats stats = systemTemplatesService.stats();
            assertNotNull(stats);
            assertEquals(stats.getTemplatesLoaded(), iter % 2);
            assertEquals(stats.getFailedLoadingTemplates(), 0L);
            assertEquals(stats.getFailedLoadingRepositories(), iter % 2);
        }
    }

    @LockFeatureFlag(APPLICATION_BASED_CONFIGURATION_TEMPLATES)
    public void testSystemTemplatesVerifyAndLoad() throws IOException {
        setupService(false);

        systemTemplatesService.verifyRepositories();
        SystemTemplatesService.Stats stats = systemTemplatesService.stats();
        assertNotNull(stats);
        assertEquals(stats.getTemplatesLoaded(), 0L);
        assertEquals(stats.getFailedLoadingTemplates(), 0L);
        assertEquals(stats.getFailedLoadingRepositories(), 0L);

        systemTemplatesService.onClusterManager();
        stats = systemTemplatesService.stats();
        assertNotNull(stats);
        assertEquals(stats.getTemplatesLoaded(), 1L);
        assertEquals(stats.getFailedLoadingTemplates(), 0L);
        assertEquals(stats.getFailedLoadingRepositories(), 0L);
    }

    @LockFeatureFlag(APPLICATION_BASED_CONFIGURATION_TEMPLATES)
    public void testSystemTemplatesVerifyWithFailingRepository() throws IOException {
        setupService(true);

        // Do it multiple times to ensure verify checks are always executed.
        for (int i = 0; i < 2; i++) {
            assertThrows(IllegalStateException.class, () -> systemTemplatesService.verifyRepositories());

            SystemTemplatesService.Stats stats = systemTemplatesService.stats();
            assertNotNull(stats);
            assertEquals(stats.getTemplatesLoaded(), 0L);
            assertEquals(stats.getFailedLoadingTemplates(), 0L);
            assertEquals(stats.getFailedLoadingRepositories(), 1L);
        }
    }

    private void setupService(boolean errorFromMockPlugin) throws IOException {
        ThreadPool mockPool = Mockito.mock(ThreadPool.class);
        when(mockPool.generic()).thenReturn(DensityExecutors.newDirectExecutorService());

        List<SystemTemplatesPlugin> plugins = new ArrayList<>();
        plugins.add(new TestSystemTemplatesRepositoryPlugin());

        if (errorFromMockPlugin) {
            SystemTemplatesPlugin mockPlugin = Mockito.mock(SystemTemplatesPlugin.class);
            when(mockPlugin.loadRepository()).thenThrow(new IOException());
            plugins.add(mockPlugin);
        }

        ClusterSettings mockSettings = new ClusterSettings(Settings.EMPTY, BUILT_IN_CLUSTER_SETTINGS);
        systemTemplatesService = new SystemTemplatesService(
            plugins,
            mockPool,
            mockSettings,
            Settings.builder().put(SystemTemplatesService.SETTING_APPLICATION_BASED_CONFIGURATION_TEMPLATES_ENABLED.getKey(), true).build()
        );
    }
}
