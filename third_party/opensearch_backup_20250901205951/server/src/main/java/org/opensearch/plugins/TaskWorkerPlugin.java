/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.plugins;

import org.density.cluster.service.ClusterService;
import org.density.common.annotation.ExperimentalApi;
import org.density.task.commons.task.TaskType;
import org.density.task.commons.worker.TaskWorker;
import org.density.threadpool.ThreadPool;
import org.density.transport.client.Client;

/**
 * Plugin for providing TaskWorkers for Offline Nodes
 */
@ExperimentalApi
public interface TaskWorkerPlugin {

    /**
     * Get the new TaskWorker for a TaskType
     *
     * @return TaskWorker to execute Tasks on Offline Nodes
     */
    TaskWorker getTaskWorker(Client client, ClusterService clusterService, ThreadPool threadPool);

    /**
     * Get the TaskType for this TaskWorker
     * @return TaskType
     */
    TaskType getTaskType();
}
