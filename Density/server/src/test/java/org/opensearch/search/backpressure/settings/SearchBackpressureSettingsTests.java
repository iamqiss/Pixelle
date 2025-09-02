/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.search.backpressure.settings;

import org.density.common.settings.ClusterSettings;
import org.density.common.settings.Settings;
import org.density.test.DensityTestCase;

public class SearchBackpressureSettingsTests extends DensityTestCase {

    /**
     * Validate proper construction of SearchBackpressureSettings object with a valid mode.
     */
    public void testSearchBackpressureSettings() {
        Settings settings = Settings.builder().put("search_backpressure.mode", "monitor_only").build();
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        SearchBackpressureSettings sbs = new SearchBackpressureSettings(settings, cs);
        assertEquals(SearchBackpressureMode.MONITOR_ONLY, sbs.getMode());
        assertEquals(settings, sbs.getSettings());
        assertEquals(cs, sbs.getClusterSettings());
    }

    /**
     * Validate construction of SearchBackpressureSettings object gets rejected
     * on invalid SearchBackpressureMode value.
     */
    public void testSearchBackpressureSettingValidateInvalidMode() {
        Settings settings = Settings.builder().put("search_backpressure.mode", "foo").build();
        assertThrows(
            IllegalArgumentException.class,
            () -> new SearchBackpressureSettings(settings, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS))
        );
    }

    public void testInvalidCancellationRate() {
        Settings settings1 = Settings.builder().put("search_backpressure.search_task.cancellation_rate", randomFrom(-1, 0)).build();
        assertThrows(
            IllegalArgumentException.class,
            () -> new SearchBackpressureSettings(settings1, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS))
        );

        Settings settings2 = Settings.builder().put("search_backpressure.search_shard_task.cancellation_rate", randomFrom(-1, 0)).build();
        assertThrows(
            IllegalArgumentException.class,
            () -> new SearchBackpressureSettings(settings2, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS))
        );
    }

    public void testInvalidCancellationRatio() {
        Settings settings1 = Settings.builder().put("search_backpressure.search_task.cancellation_ratio", randomFrom(-1, 0)).build();
        assertThrows(
            IllegalArgumentException.class,
            () -> new SearchBackpressureSettings(settings1, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS))
        );

        Settings settings2 = Settings.builder().put("search_backpressure.search_shard_task.cancellation_ratio", randomFrom(-1, 0)).build();
        assertThrows(
            IllegalArgumentException.class,
            () -> new SearchBackpressureSettings(settings2, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS))
        );
    }
}
