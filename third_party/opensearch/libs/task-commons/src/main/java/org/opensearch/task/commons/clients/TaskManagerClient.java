/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.task.commons.clients;

import org.density.common.annotation.ExperimentalApi;
import org.density.task.commons.task.Task;
import org.density.task.commons.task.TaskId;
import org.density.task.commons.worker.WorkerNode;

import java.util.List;

/**
 * Client used to interact with Task Store/Queue.
 *
 * TODO: TaskManager can be something not running an density process.
 * We need to come up with a way to allow this interface to be used with in and out density as well
 *
 * @density.experimental
 */
@ExperimentalApi
public interface TaskManagerClient {

    /**
     * Get task from TaskStore/Queue
     *
     * @param taskId TaskId of the task to be retrieved
     * @return Task corresponding to TaskId
     */
    Task getTask(TaskId taskId);

    /**
     * Update task in TaskStore/Queue
     *
     * @param task Task to be updated
     */
    void updateTask(Task task);

    /**
     * Mark task as cancelled.
     * Ongoing Tasks can be cancelled as well if the corresponding worker supports cancellation
     *
     * @param taskId TaskId of the task to be cancelled
     */
    void cancelTask(TaskId taskId);

    /**
     * List all tasks applying all the filters present in listTaskRequest
     *
     * @param taskListRequest TaskListRequest
     * @return list of all the task matching the filters in listTaskRequest
     */
    List<Task> listTasks(TaskListRequest taskListRequest);

    /**
     * Assign Task to a particular WorkerNode. This ensures no 2 worker Nodes work on the same task.
     * This API can be used in both pull and push models of task assignment.
     *
     * @param taskId TaskId of the task to be assigned
     * @param node WorkerNode task is being assigned to
     * @return true if task is assigned successfully, false otherwise
     */
    boolean assignTask(TaskId taskId, WorkerNode node);
}
