/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.search.backpressure.stats;

import org.density.core.common.io.stream.Writeable;
import org.density.search.backpressure.trackers.CpuUsageTracker;
import org.density.search.backpressure.trackers.ElapsedTimeTracker;
import org.density.search.backpressure.trackers.HeapUsageTracker;
import org.density.search.backpressure.trackers.TaskResourceUsageTrackerType;
import org.density.search.backpressure.trackers.TaskResourceUsageTrackers.TaskResourceUsageTracker;
import org.density.test.AbstractWireSerializingTestCase;

import java.util.Map;

public class SearchTaskStatsTests extends AbstractWireSerializingTestCase<SearchTaskStats> {

    @Override
    protected Writeable.Reader<SearchTaskStats> instanceReader() {
        return SearchTaskStats::new;
    }

    @Override
    protected SearchTaskStats createTestInstance() {
        return randomInstance();
    }

    public static SearchTaskStats randomInstance() {
        Map<TaskResourceUsageTrackerType, TaskResourceUsageTracker.Stats> resourceUsageTrackerStats = Map.of(
            TaskResourceUsageTrackerType.CPU_USAGE_TRACKER,
            new CpuUsageTracker.Stats(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong()),
            TaskResourceUsageTrackerType.HEAP_USAGE_TRACKER,
            new HeapUsageTracker.Stats(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong()),
            TaskResourceUsageTrackerType.ELAPSED_TIME_TRACKER,
            new ElapsedTimeTracker.Stats(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong())
        );

        return new SearchTaskStats(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong(), resourceUsageTrackerStats);
    }
}
