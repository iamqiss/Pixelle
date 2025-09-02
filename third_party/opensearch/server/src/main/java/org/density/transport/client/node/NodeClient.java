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

package org.density.transport.client.node;

import org.density.action.ActionModule.DynamicActionRegistry;
import org.density.action.ActionRequest;
import org.density.action.ActionType;
import org.density.action.search.SearchRequestBuilder;
import org.density.action.support.TransportAction;
import org.density.cluster.node.DiscoveryNode;
import org.density.common.annotation.PublicApi;
import org.density.common.settings.Settings;
import org.density.core.action.ActionListener;
import org.density.core.action.ActionResponse;
import org.density.core.common.io.stream.NamedWriteableRegistry;
import org.density.tasks.Task;
import org.density.tasks.TaskListener;
import org.density.threadpool.ThreadPool;
import org.density.transport.RemoteClusterService;
import org.density.transport.client.Client;
import org.density.transport.client.support.AbstractClient;

import java.util.function.Supplier;

/**
 * Client that executes actions on the local node.
 *
 * @density.api
 */
@PublicApi(since = "1.0.0")
public class NodeClient extends AbstractClient {

    private DynamicActionRegistry actionRegistry;
    /**
     * The id of the local {@link DiscoveryNode}. Useful for generating task ids from tasks returned by
     * {@link #executeLocally(ActionType, ActionRequest, TaskListener)}.
     */
    private Supplier<String> localNodeId;
    private RemoteClusterService remoteClusterService;
    private NamedWriteableRegistry namedWriteableRegistry;

    public NodeClient(Settings settings, ThreadPool threadPool) {
        super(settings, threadPool);
    }

    public void initialize(
        DynamicActionRegistry actionRegistry,
        Supplier<String> localNodeId,
        RemoteClusterService remoteClusterService,
        NamedWriteableRegistry namedWriteableRegistry
    ) {
        this.actionRegistry = actionRegistry;
        this.localNodeId = localNodeId;
        this.remoteClusterService = remoteClusterService;
        this.namedWriteableRegistry = namedWriteableRegistry;
    }

    @Override
    public void close() {
        // nothing really to do
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
        ActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    ) {
        // Discard the task because the Client interface doesn't use it.
        executeLocally(action, request, listener);
    }

    /**
     * Execute an {@link ActionType} locally, returning that {@link Task} used to track it, and linking an {@link ActionListener}.
     * Prefer this method if you don't need access to the task when listening for the response. This is the method used to implement
     * the {@link Client} interface.
     */
    public <Request extends ActionRequest, Response extends ActionResponse> Task executeLocally(
        ActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    ) {
        return transportAction(action).execute(request, listener);
    }

    /**
     * Execute an {@link ActionType} locally, returning that {@link Task} used to track it, and linking an {@link TaskListener}. Prefer this
     * method if you need access to the task when listening for the response.
     */
    public <Request extends ActionRequest, Response extends ActionResponse> Task executeLocally(
        ActionType<Response> action,
        Request request,
        TaskListener<Response> listener
    ) {
        return transportAction(action).execute(request, listener);
    }

    /**
     * The id of the local {@link DiscoveryNode}. Useful for generating task ids from tasks returned by
     * {@link #executeLocally(ActionType, ActionRequest, TaskListener)}.
     */
    public String getLocalNodeId() {
        return localNodeId.get();
    }

    /**
     * Get the {@link TransportAction} for an {@link ActionType}, throwing exceptions if the action isn't available.
     */
    @SuppressWarnings("unchecked")
    private <Request extends ActionRequest, Response extends ActionResponse> TransportAction<Request, Response> transportAction(
        ActionType<Response> action
    ) {
        if (actionRegistry == null) {
            throw new IllegalStateException("NodeClient has not been initialized");
        }
        TransportAction<Request, Response> transportAction = (TransportAction<Request, Response>) actionRegistry.get(action);
        if (transportAction == null) {
            throw new IllegalStateException("failed to find action [" + action + "] to execute");
        }
        return transportAction;
    }

    @Override
    public Client getRemoteClusterClient(String clusterAlias) {
        return remoteClusterService.getRemoteClusterClient(threadPool(), clusterAlias);
    }

    public NamedWriteableRegistry getNamedWriteableRegistry() {
        return namedWriteableRegistry;
    }

    @Override
    public SearchRequestBuilder prepareStreamSearch(String... indices) {
        return super.prepareStreamSearch(indices);
    }
}
