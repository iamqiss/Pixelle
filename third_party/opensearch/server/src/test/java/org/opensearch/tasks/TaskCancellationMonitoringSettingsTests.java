/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.tasks;

import org.density.common.settings.ClusterSettings;
import org.density.common.settings.Settings;
import org.density.test.DensityTestCase;

public class TaskCancellationMonitoringSettingsTests extends DensityTestCase {

    public void testDefaults() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        TaskCancellationMonitoringSettings settings = new TaskCancellationMonitoringSettings(Settings.EMPTY, clusterSettings);
        assertEquals(TaskCancellationMonitoringSettings.DURATION_MILLIS_SETTING_DEFAULT_VALUE, settings.getDuration().millis());
        assertEquals(TaskCancellationMonitoringSettings.INTERVAL_MILLIS_SETTING_DEFAULT_VALUE, settings.getInterval().millis());
        assertEquals(TaskCancellationMonitoringSettings.IS_ENABLED_SETTING_DEFAULT_VALUE, settings.isEnabled());
    }

    public void testUpdate() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        TaskCancellationMonitoringSettings settings = new TaskCancellationMonitoringSettings(Settings.EMPTY, clusterSettings);

        Settings newSettings = Settings.builder()
            .put(TaskCancellationMonitoringSettings.DURATION_MILLIS_SETTING.getKey(), 20000)
            .put(TaskCancellationMonitoringSettings.INTERVAL_MILLIS_SETTING.getKey(), 2000)
            .put(TaskCancellationMonitoringSettings.IS_ENABLED_SETTING.getKey(), false)
            .build();
        clusterSettings.applySettings(newSettings);
        assertEquals(20000, settings.getDuration().millis());
        assertFalse(settings.isEnabled());
        assertNotEquals(2000, settings.getInterval().millis());
    }
}
