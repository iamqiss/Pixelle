/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.wlm.listeners;

import org.density.action.search.SearchPhaseContext;
import org.density.action.search.SearchRequestContext;
import org.density.action.search.SearchRequestOperationsListener;
import org.density.threadpool.ThreadPool;
import org.density.wlm.WorkloadGroupService;
import org.density.wlm.WorkloadGroupTask;

/**
 * This listener is used to listen for request lifecycle events for a workloadGroup
 */
public class WorkloadGroupRequestOperationListener extends SearchRequestOperationsListener {

    private final WorkloadGroupService workloadGroupService;
    private final ThreadPool threadPool;

    public WorkloadGroupRequestOperationListener(WorkloadGroupService workloadGroupService, ThreadPool threadPool) {
        this.workloadGroupService = workloadGroupService;
        this.threadPool = threadPool;
    }

    /**
     * This method assumes that the workloadGroupId is already populated in the thread context
     * @param searchRequestContext SearchRequestContext instance
     */
    @Override
    protected void onRequestStart(SearchRequestContext searchRequestContext) {
        final String workloadGroupId = threadPool.getThreadContext().getHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER);
        workloadGroupService.rejectIfNeeded(workloadGroupId);
    }

    @Override
    protected void onRequestFailure(SearchPhaseContext context, SearchRequestContext searchRequestContext) {
        final String workloadGroupId = threadPool.getThreadContext().getHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER);
        workloadGroupService.incrementFailuresFor(workloadGroupId);
    }
}
