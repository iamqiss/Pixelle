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
import org.density.task.commons.clients.TaskManagerClient;
import org.density.threadpool.ThreadPool;
import org.density.transport.client.Client;

/**
 * Plugin to provide an implementation of Task client
 */
@ExperimentalApi
public interface TaskManagerClientPlugin {

    /**
     * Get the task client.
     */
    TaskManagerClient getTaskManagerClient(Client client, ClusterService clusterService, ThreadPool threadPool);
}
