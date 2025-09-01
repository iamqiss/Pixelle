/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.wlm.tracker;

import org.density.action.search.SearchShardTask;
import org.density.common.util.concurrent.ThreadContext;
import org.density.core.tasks.resourcetracker.ResourceStats;
import org.density.tasks.Task;
import org.density.tasks.TaskResourceTrackingService;
import org.density.test.DensityTestCase;
import org.density.threadpool.TestThreadPool;
import org.density.threadpool.ThreadPool;
import org.density.wlm.ResourceType;
import org.density.wlm.WorkloadGroupLevelResourceUsageView;
import org.density.wlm.WorkloadGroupTask;
import org.density.wlm.WorkloadManagementSettings;
import org.junit.After;
import org.junit.Before;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.density.wlm.WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER;
import static org.density.wlm.cancellation.WorkloadGroupTaskCancellationService.MIN_VALUE;
import static org.density.wlm.tracker.CpuUsageCalculator.PROCESSOR_COUNT;
import static org.density.wlm.tracker.MemoryUsageCalculator.HEAP_SIZE_BYTES;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ResourceUsageCalculatorTrackerServiceTests extends DensityTestCase {
    TestThreadPool threadPool;
    TaskResourceTrackingService mockTaskResourceTrackingService;
    WorkloadGroupResourceUsageTrackerService workloadGroupResourceUsageTrackerService;
    WorkloadManagementSettings settings;

    public static class TestClock {
        long time;

        public void fastForwardBy(long nanos) {
            time += nanos;
        }

        public long getTime() {
            return time;
        }
    }

    TestClock clock;

    @Before
    public void setup() {
        clock = new TestClock();
        settings = mock(WorkloadManagementSettings.class);
        threadPool = new TestThreadPool(getTestName());
        mockTaskResourceTrackingService = mock(TaskResourceTrackingService.class);
        workloadGroupResourceUsageTrackerService = new WorkloadGroupResourceUsageTrackerService(mockTaskResourceTrackingService);
    }

    @After
    public void cleanup() {
        ThreadPool.terminate(threadPool, 5, TimeUnit.SECONDS);
    }

    public void testConstructWorkloadGroupLevelViews_CreatesWorkloadGroupLevelUsageView_WhenTasksArePresent() {
        List<String> workloadGroupIds = List.of("workloadGroup1", "workloadGroup2", "workloadGroup3");
        clock.fastForwardBy(2000);

        Map<Long, Task> activeSearchShardTasks = createActiveSearchShardTasks(workloadGroupIds);
        when(mockTaskResourceTrackingService.getResourceAwareTasks()).thenReturn(activeSearchShardTasks);

        Map<String, WorkloadGroupLevelResourceUsageView> stringWorkloadGroupLevelResourceUsageViewMap =
            workloadGroupResourceUsageTrackerService.constructWorkloadGroupLevelUsageViews();

        for (String workloadGroupId : workloadGroupIds) {
            assertEquals(
                (400 * 1.0f) / HEAP_SIZE_BYTES,
                stringWorkloadGroupLevelResourceUsageViewMap.get(workloadGroupId).getResourceUsageData().get(ResourceType.MEMORY),
                MIN_VALUE
            );
            assertEquals(
                (200 * 1.0f) / (PROCESSOR_COUNT * 2000),
                stringWorkloadGroupLevelResourceUsageViewMap.get(workloadGroupId).getResourceUsageData().get(ResourceType.CPU),
                MIN_VALUE
            );
            assertEquals(2, stringWorkloadGroupLevelResourceUsageViewMap.get(workloadGroupId).getActiveTasks().size());
        }
    }

    public void testConstructWorkloadGroupLevelViews_CreatesWorkloadGroupLevelUsageView_WhenTasksAreNotPresent() {
        Map<String, WorkloadGroupLevelResourceUsageView> stringWorkloadGroupLevelResourceUsageViewMap =
            workloadGroupResourceUsageTrackerService.constructWorkloadGroupLevelUsageViews();
        assertTrue(stringWorkloadGroupLevelResourceUsageViewMap.isEmpty());
    }

    public void testConstructWorkloadGroupLevelUsageViews_WithTasksHavingDifferentResourceUsage() {
        Map<Long, Task> activeSearchShardTasks = new HashMap<>();
        clock.fastForwardBy(2000);
        activeSearchShardTasks.put(1L, createMockTask(SearchShardTask.class, 100, 200, "workloadGroup1"));
        activeSearchShardTasks.put(2L, createMockTask(SearchShardTask.class, 200, 400, "workloadGroup1"));
        when(mockTaskResourceTrackingService.getResourceAwareTasks()).thenReturn(activeSearchShardTasks);
        Map<String, WorkloadGroupLevelResourceUsageView> workloadGroupViews = workloadGroupResourceUsageTrackerService
            .constructWorkloadGroupLevelUsageViews();

        assertEquals(
            (double) 600 / HEAP_SIZE_BYTES,
            workloadGroupViews.get("workloadGroup1").getResourceUsageData().get(ResourceType.MEMORY),
            MIN_VALUE
        );
        assertEquals(
            ((double) 300) / (PROCESSOR_COUNT * 2000),
            workloadGroupViews.get("workloadGroup1").getResourceUsageData().get(ResourceType.CPU),
            MIN_VALUE
        );
        assertEquals(2, workloadGroupViews.get("workloadGroup1").getActiveTasks().size());
    }

    private Map<Long, Task> createActiveSearchShardTasks(List<String> workloadGroupIds) {
        Map<Long, Task> activeSearchShardTasks = new HashMap<>();
        long task_id = 0;
        for (String workloadGroupId : workloadGroupIds) {
            for (int i = 0; i < 2; i++) {
                activeSearchShardTasks.put(++task_id, createMockTask(SearchShardTask.class, 100, 200, workloadGroupId));
            }
        }
        return activeSearchShardTasks;
    }

    private <T extends WorkloadGroupTask> T createMockTask(Class<T> type, long cpuUsage, long heapUsage, String workloadGroupId) {
        T task = mock(type);
        try (ThreadContext.StoredContext ignore = threadPool.getThreadContext().stashContext()) {
            threadPool.getThreadContext().putHeader(WORKLOAD_GROUP_ID_HEADER, workloadGroupId);
            task.setWorkloadGroupId(threadPool.getThreadContext());
        }
        when(task.getTotalResourceUtilization(ResourceStats.CPU)).thenReturn(cpuUsage);
        when(task.getTotalResourceUtilization(ResourceStats.MEMORY)).thenReturn(heapUsage);
        when(task.getStartTimeNanos()).thenReturn((long) 0);
        when(task.getElapsedTime()).thenReturn(clock.getTime());
        when(task.isWorkloadGroupSet()).thenReturn(true);

        AtomicBoolean isCancelled = new AtomicBoolean(false);
        doAnswer(invocation -> {
            isCancelled.set(true);
            return null;
        }).when(task).cancel(anyString());
        doAnswer(invocation -> isCancelled.get()).when(task).isCancelled();

        return task;
    }
}
