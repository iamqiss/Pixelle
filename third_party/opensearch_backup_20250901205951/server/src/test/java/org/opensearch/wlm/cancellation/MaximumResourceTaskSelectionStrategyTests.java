/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.wlm.cancellation;

import org.density.action.search.SearchAction;
import org.density.action.search.SearchTask;
import org.density.core.tasks.TaskId;
import org.density.core.tasks.resourcetracker.ResourceStats;
import org.density.core.tasks.resourcetracker.ResourceStatsType;
import org.density.core.tasks.resourcetracker.ResourceUsageMetric;
import org.density.test.DensityTestCase;
import org.density.wlm.ResourceType;
import org.density.wlm.WorkloadGroupTask;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

import static org.density.wlm.cancellation.WorkloadGroupTaskCancellationService.MIN_VALUE;
import static org.density.wlm.tracker.MemoryUsageCalculator.HEAP_SIZE_BYTES;

public class MaximumResourceTaskSelectionStrategyTests extends DensityTestCase {

    public void testSelectTasksToCancelSelectsTasksMeetingThreshold_ifReduceByIsGreaterThanZero() {
        MaximumResourceTaskSelectionStrategy testHighestResourceConsumingTaskFirstSelectionStrategy =
            new MaximumResourceTaskSelectionStrategy();
        double reduceBy = 50000.0 / HEAP_SIZE_BYTES;
        ResourceType resourceType = ResourceType.MEMORY;
        List<WorkloadGroupTask> tasks = getListOfTasks(100);
        List<WorkloadGroupTask> selectedTasks = testHighestResourceConsumingTaskFirstSelectionStrategy.selectTasksForCancellation(
            tasks,
            reduceBy,
            resourceType
        );
        assertFalse(selectedTasks.isEmpty());
        boolean sortedInDescendingResourceUsage = IntStream.range(0, selectedTasks.size() - 1)
            .noneMatch(
                index -> ResourceType.MEMORY.getResourceUsageCalculator()
                    .calculateTaskResourceUsage(selectedTasks.get(index)) < ResourceType.MEMORY.getResourceUsageCalculator()
                        .calculateTaskResourceUsage(selectedTasks.get(index + 1))
            );
        assertTrue(sortedInDescendingResourceUsage);
        assertTrue(tasksUsageMeetsThreshold(selectedTasks, reduceBy));
    }

    public void testSelectTasksToCancelSelectsTasksMeetingThreshold_ifReduceByIsLesserThanZero() {
        MaximumResourceTaskSelectionStrategy testHighestResourceConsumingTaskFirstSelectionStrategy =
            new MaximumResourceTaskSelectionStrategy();
        double reduceBy = -50.0 / HEAP_SIZE_BYTES;
        ResourceType resourceType = ResourceType.MEMORY;
        List<WorkloadGroupTask> tasks = getListOfTasks(3);
        try {
            testHighestResourceConsumingTaskFirstSelectionStrategy.selectTasksForCancellation(tasks, reduceBy, resourceType);
        } catch (Exception e) {
            assertTrue(e instanceof IllegalArgumentException);
            assertEquals("limit has to be greater than zero", e.getMessage());
        }
    }

    public void testSelectTasksToCancelSelectsTasksMeetingThreshold_ifReduceByIsEqualToZero() {
        MaximumResourceTaskSelectionStrategy testHighestResourceConsumingTaskFirstSelectionStrategy =
            new MaximumResourceTaskSelectionStrategy();
        double reduceBy = 0.0;
        ResourceType resourceType = ResourceType.MEMORY;
        List<WorkloadGroupTask> tasks = getListOfTasks(50);
        List<WorkloadGroupTask> selectedTasks = testHighestResourceConsumingTaskFirstSelectionStrategy.selectTasksForCancellation(
            tasks,
            reduceBy,
            resourceType
        );
        assertTrue(selectedTasks.isEmpty());
    }

    private boolean tasksUsageMeetsThreshold(List<WorkloadGroupTask> selectedTasks, double threshold) {
        double memory = 0;
        for (WorkloadGroupTask task : selectedTasks) {
            memory += ResourceType.MEMORY.getResourceUsageCalculator().calculateTaskResourceUsage(task);
            if ((memory - threshold) > MIN_VALUE) {
                return true;
            }
        }
        return false;
    }

    private List<WorkloadGroupTask> getListOfTasks(int numberOfTasks) {
        List<WorkloadGroupTask> tasks = new ArrayList<>();

        while (tasks.size() < numberOfTasks) {
            long id = randomLong();
            final WorkloadGroupTask task = getRandomSearchTask(id);
            long initial_memory = randomLongBetween(1, 100);

            ResourceUsageMetric[] initialTaskResourceMetrics = new ResourceUsageMetric[] {
                new ResourceUsageMetric(ResourceStats.MEMORY, initial_memory) };
            task.startThreadResourceTracking(id, ResourceStatsType.WORKER_STATS, initialTaskResourceMetrics);

            long memory = initial_memory + randomLongBetween(1, 10000);

            ResourceUsageMetric[] taskResourceMetrics = new ResourceUsageMetric[] {
                new ResourceUsageMetric(ResourceStats.MEMORY, memory), };
            task.updateThreadResourceStats(id, ResourceStatsType.WORKER_STATS, taskResourceMetrics);
            task.stopThreadResourceTracking(id, ResourceStatsType.WORKER_STATS);
            tasks.add(task);
        }

        return tasks;
    }

    private WorkloadGroupTask getRandomSearchTask(long id) {
        return new SearchTask(
            id,
            "transport",
            SearchAction.NAME,
            () -> "test description",
            new TaskId(randomLong() + ":" + randomLong()),
            Collections.emptyMap()
        );
    }
}
