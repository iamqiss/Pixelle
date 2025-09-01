/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.wlm.tracker;

import org.density.common.annotation.PublicApi;
import org.density.wlm.WorkloadGroupTask;

import java.util.List;

/**
 * This class is used to track workload group level resource usage
 */
@PublicApi(since = "2.18.0")
public abstract class ResourceUsageCalculator {
    /**
     * calculates the current resource usage for the workload group
     *
     * @param tasks        list of tasks in the workload group
     */
    public abstract double calculateResourceUsage(List<WorkloadGroupTask> tasks);

    /**
     * calculates the task level resource usage
     * @param task         WorkloadGroupTask
     * @return task level resource usage
     */
    public abstract double calculateTaskResourceUsage(WorkloadGroupTask task);
}
