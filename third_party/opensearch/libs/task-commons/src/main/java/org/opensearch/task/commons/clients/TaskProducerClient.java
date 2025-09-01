/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.task.commons.clients;

import org.density.task.commons.task.Task;

/**
 * Producer interface used to submit new tasks for execution on worker nodes.
 */
public interface TaskProducerClient {

    /**
     * Submit a new task to TaskStore/Queue
     *
     * @param task Task to be submitted for execution on offline nodes
     */
    void submitTask(Task task);
}
