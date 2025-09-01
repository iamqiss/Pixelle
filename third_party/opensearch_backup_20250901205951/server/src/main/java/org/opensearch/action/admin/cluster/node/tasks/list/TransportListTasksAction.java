/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright Density Contributors. See
 * GitHub history for details.
 */

package org.density.action.admin.cluster.node.tasks.list;

import org.density.action.FailedNodeException;
import org.density.action.TaskOperationFailure;
import org.density.action.support.ActionFilters;
import org.density.action.support.tasks.TransportTasksAction;
import org.density.cluster.service.ClusterService;
import org.density.common.inject.Inject;
import org.density.common.unit.TimeValue;
import org.density.core.action.ActionListener;
import org.density.tasks.Task;
import org.density.tasks.TaskInfo;
import org.density.tasks.TaskResourceTrackingService;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportService;

import java.util.List;
import java.util.function.Consumer;

import static org.density.common.unit.TimeValue.timeValueSeconds;

/**
 * Transport action for listing tasks currently running on the nodes
 *
 * @density.internal
 */
public class TransportListTasksAction extends TransportTasksAction<Task, ListTasksRequest, ListTasksResponse, TaskInfo> {
    public static long waitForCompletionTimeout(TimeValue timeout) {
        if (timeout == null) {
            timeout = DEFAULT_WAIT_FOR_COMPLETION_TIMEOUT;
        }
        return System.nanoTime() + timeout.nanos();
    }

    private static final TimeValue DEFAULT_WAIT_FOR_COMPLETION_TIMEOUT = timeValueSeconds(30);

    private final TaskResourceTrackingService taskResourceTrackingService;

    @Inject
    public TransportListTasksAction(
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        TaskResourceTrackingService taskResourceTrackingService
    ) {
        super(
            ListTasksAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            ListTasksRequest::new,
            ListTasksResponse::new,
            TaskInfo::new,
            ThreadPool.Names.MANAGEMENT
        );
        this.taskResourceTrackingService = taskResourceTrackingService;
    }

    @Override
    protected ListTasksResponse newResponse(
        ListTasksRequest request,
        List<TaskInfo> tasks,
        List<TaskOperationFailure> taskOperationFailures,
        List<FailedNodeException> failedNodeExceptions
    ) {
        return new ListTasksResponse(tasks, taskOperationFailures, failedNodeExceptions);
    }

    @Override
    protected void taskOperation(ListTasksRequest request, Task task, ActionListener<TaskInfo> listener) {
        listener.onResponse(task.taskInfo(clusterService.localNode().getId(), request.getDetailed()));
    }

    @Override
    protected void processTasks(ListTasksRequest request, Consumer<Task> operation) {
        if (request.getWaitForCompletion()) {
            long timeoutNanos = waitForCompletionTimeout(request.getTimeout());
            operation = operation.andThen(task -> {
                if (task.getAction().startsWith(ListTasksAction.NAME)) {
                    // It doesn't make sense to wait for List Tasks and it can cause an infinite loop of the task waiting
                    // for itself or one of its child tasks
                    return;
                }
                taskManager.waitForTaskCompletion(task, timeoutNanos);
            });
        } else {
            operation = operation.andThen(taskResourceTrackingService::refreshResourceStats);
        }
        super.processTasks(request, operation);
    }

}
