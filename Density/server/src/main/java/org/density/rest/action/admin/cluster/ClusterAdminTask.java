/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.rest.action.admin.cluster;

import org.density.common.annotation.PublicApi;
import org.density.common.unit.TimeValue;
import org.density.core.tasks.TaskId;
import org.density.tasks.CancellableTask;

import java.util.Map;

import static org.density.search.SearchService.NO_TIMEOUT;

/**
 * Task storing information about a currently running ClusterRequest.
 *
 * @density.api
 */
@PublicApi(since = "2.17.0")
public class ClusterAdminTask extends CancellableTask {

    public ClusterAdminTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        this(id, type, action, parentTaskId, headers, NO_TIMEOUT);
    }

    public ClusterAdminTask(
        long id,
        String type,
        String action,
        TaskId parentTaskId,
        Map<String, String> headers,
        TimeValue cancelAfterTimeInterval
    ) {
        super(id, type, action, null, parentTaskId, headers, cancelAfterTimeInterval);
    }

    @Override
    public boolean shouldCancelChildrenOnCancellation() {
        return true;
    }
}
