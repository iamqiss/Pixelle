/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.wlm.tracker;

import org.density.action.search.SearchTask;
import org.density.common.settings.ClusterSettings;
import org.density.common.settings.Settings;
import org.density.common.util.concurrent.ThreadContext;
import org.density.core.tasks.TaskId;
import org.density.tasks.TaskResourceTrackingService;
import org.density.test.DensityTestCase;
import org.density.threadpool.TestThreadPool;
import org.density.threadpool.ThreadPool;
import org.density.wlm.WorkloadGroupLevelResourceUsageView;
import org.density.wlm.WorkloadGroupTask;

import java.util.HashMap;
import java.util.Map;

public class WorkloadGroupTaskResourceTrackingTests extends DensityTestCase {
    ThreadPool threadPool;
    WorkloadGroupResourceUsageTrackerService workloadGroupResourceUsageTrackerService;
    TaskResourceTrackingService taskResourceTrackingService;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool("workload-management-tracking-thread-pool");
        taskResourceTrackingService = new TaskResourceTrackingService(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            threadPool
        );
        workloadGroupResourceUsageTrackerService = new WorkloadGroupResourceUsageTrackerService(taskResourceTrackingService);
    }

    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdownNow();
    }

    public void testValidWorkloadGroupTasksCase() {
        taskResourceTrackingService.setTaskResourceTrackingEnabled(true);
        WorkloadGroupTask task = new SearchTask(1, "test", "test", () -> "Test", TaskId.EMPTY_TASK_ID, new HashMap<>());
        taskResourceTrackingService.startTracking(task);

        // since the workload group id is not set we should not track this task
        Map<String, WorkloadGroupLevelResourceUsageView> resourceUsageViewMap = workloadGroupResourceUsageTrackerService
            .constructWorkloadGroupLevelUsageViews();
        assertTrue(resourceUsageViewMap.isEmpty());

        // Now since this task has a valid workloadGroupId header it should be tracked
        try (ThreadContext.StoredContext context = threadPool.getThreadContext().stashContext()) {
            threadPool.getThreadContext().putHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, "testHeader");
            task.setWorkloadGroupId(threadPool.getThreadContext());
            resourceUsageViewMap = workloadGroupResourceUsageTrackerService.constructWorkloadGroupLevelUsageViews();
            assertFalse(resourceUsageViewMap.isEmpty());
        }
    }
}
