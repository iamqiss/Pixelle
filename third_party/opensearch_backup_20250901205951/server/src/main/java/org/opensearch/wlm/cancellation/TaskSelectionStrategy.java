/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.wlm.cancellation;

import org.density.wlm.ResourceType;
import org.density.wlm.WorkloadGroupTask;

import java.util.List;

/**
 * This interface exposes a method which implementations can use
 */
public interface TaskSelectionStrategy {
    /**
     * Determines how the tasks are selected from the list of given tasks based on resource type
     * @param tasks to select from
     * @param limit min cumulative resource usage sum of selected tasks
     * @param resourceType
     * @return list of tasks
     */
    List<WorkloadGroupTask> selectTasksForCancellation(List<WorkloadGroupTask> tasks, double limit, ResourceType resourceType);
}
