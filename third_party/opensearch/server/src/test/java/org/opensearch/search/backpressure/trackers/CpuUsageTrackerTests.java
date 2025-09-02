/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.search.backpressure.trackers;

import org.density.action.search.SearchShardTask;
import org.density.action.search.SearchTask;
import org.density.common.settings.ClusterSettings;
import org.density.common.settings.Settings;
import org.density.search.backpressure.settings.SearchBackpressureSettings;
import org.density.search.backpressure.settings.SearchShardTaskSettings;
import org.density.search.backpressure.settings.SearchTaskSettings;
import org.density.tasks.Task;
import org.density.tasks.TaskCancellation;
import org.density.test.DensityTestCase;

import java.util.Optional;

import static org.density.search.backpressure.SearchBackpressureTestHelpers.createMockTaskWithResourceStats;

public class CpuUsageTrackerTests extends DensityTestCase {
    private static final SearchBackpressureSettings mockSettings = new SearchBackpressureSettings(
        Settings.builder()
            .put(SearchShardTaskSettings.SETTING_CPU_TIME_MILLIS_THRESHOLD.getKey(), 15)  // 15 ms
            .put(SearchTaskSettings.SETTING_CPU_TIME_MILLIS_THRESHOLD.getKey(), 25)   // 25 ms
            .build(),
        new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
    );

    public void testSearchTaskEligibleForCancellation() {
        Task task = createMockTaskWithResourceStats(SearchTask.class, 100000000, 200, randomNonNegativeLong());
        CpuUsageTracker tracker = new CpuUsageTracker(mockSettings.getSearchTaskSettings()::getCpuTimeNanosThreshold);

        Optional<TaskCancellation.Reason> reason = tracker.checkAndMaybeGetCancellationReason(task);
        assertTrue(reason.isPresent());
        assertEquals(1, reason.get().getCancellationScore());
        assertEquals("cpu usage exceeded [100ms >= 25ms]", reason.get().getMessage());
    }

    public void testSearchShardTaskEligibleForCancellation() {
        Task task = createMockTaskWithResourceStats(SearchShardTask.class, 200000000, 200, randomNonNegativeLong());
        CpuUsageTracker tracker = new CpuUsageTracker(mockSettings.getSearchShardTaskSettings()::getCpuTimeNanosThreshold);

        Optional<TaskCancellation.Reason> reason = tracker.checkAndMaybeGetCancellationReason(task);
        assertTrue(reason.isPresent());
        assertEquals(1, reason.get().getCancellationScore());
        assertEquals("cpu usage exceeded [200ms >= 15ms]", reason.get().getMessage());
    }

    public void testNotEligibleForCancellation() {
        Task task = createMockTaskWithResourceStats(SearchShardTask.class, 5000000, 200, randomNonNegativeLong());
        CpuUsageTracker tracker = new CpuUsageTracker(mockSettings.getSearchShardTaskSettings()::getCpuTimeNanosThreshold);

        Optional<TaskCancellation.Reason> reason = tracker.checkAndMaybeGetCancellationReason(task);
        assertFalse(reason.isPresent());
    }
}
