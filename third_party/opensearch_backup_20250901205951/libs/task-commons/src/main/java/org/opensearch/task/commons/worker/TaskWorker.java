/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.task.commons.worker;

import org.density.common.annotation.ExperimentalApi;
import org.density.task.commons.task.Task;

/**
 * Task Worker that executes the Task
 *
 * @density.experimental
 */
@ExperimentalApi
public interface TaskWorker {

    /**
     * Execute the Task
     *
     * @param task Task to be executed
     */
    void executeTask(Task task);

}
