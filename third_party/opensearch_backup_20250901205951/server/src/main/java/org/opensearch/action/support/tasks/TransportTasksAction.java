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

package org.density.action.support.tasks;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.density.ResourceNotFoundException;
import org.density.action.FailedNodeException;
import org.density.action.NoSuchNodeException;
import org.density.action.TaskOperationFailure;
import org.density.action.support.ActionFilters;
import org.density.action.support.HandledTransportAction;
import org.density.cluster.ClusterState;
import org.density.cluster.node.DiscoveryNode;
import org.density.cluster.node.DiscoveryNodes;
import org.density.cluster.service.ClusterService;
import org.density.common.collect.Tuple;
import org.density.common.util.concurrent.AtomicArray;
import org.density.core.action.ActionListener;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;
import org.density.core.common.io.stream.Writeable;
import org.density.core.transport.TransportResponse;
import org.density.tasks.Task;
import org.density.threadpool.ThreadPool;
import org.density.transport.NodeShouldNotConnectException;
import org.density.transport.TransportChannel;
import org.density.transport.TransportException;
import org.density.transport.TransportRequest;
import org.density.transport.TransportRequestHandler;
import org.density.transport.TransportRequestOptions;
import org.density.transport.TransportResponseHandler;
import org.density.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Consumer;

import static java.util.Collections.emptyList;

/**
 * The base class for transport actions that are interacting with currently running tasks.
 *
 * @density.internal
 */
public abstract class TransportTasksAction<
    OperationTask extends Task,
    TasksRequest extends BaseTasksRequest<TasksRequest>,
    TasksResponse extends BaseTasksResponse,
    TaskResponse extends Writeable> extends HandledTransportAction<TasksRequest, TasksResponse> {

    protected final ClusterService clusterService;
    protected final TransportService transportService;
    protected final Writeable.Reader<TasksRequest> requestReader;
    protected final Writeable.Reader<TasksResponse> responsesReader;
    protected final Writeable.Reader<TaskResponse> responseReader;

    protected final String transportNodeAction;

    protected TransportTasksAction(
        String actionName,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        Writeable.Reader<TasksRequest> requestReader,
        Writeable.Reader<TasksResponse> responsesReader,
        Writeable.Reader<TaskResponse> responseReader,
        String nodeExecutor
    ) {
        super(actionName, transportService, actionFilters, requestReader);
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.transportNodeAction = actionName + "[n]";
        this.requestReader = requestReader;
        this.responsesReader = responsesReader;
        this.responseReader = responseReader;

        transportService.registerRequestHandler(transportNodeAction, nodeExecutor, NodeTaskRequest::new, new NodeTransportHandler());
    }

    @Override
    protected void doExecute(Task task, TasksRequest request, ActionListener<TasksResponse> listener) {
        new AsyncAction(task, request, listener).start();
    }

    private void nodeOperation(NodeTaskRequest nodeTaskRequest, ActionListener<NodeTasksResponse> listener) {
        TasksRequest request = nodeTaskRequest.tasksRequest;
        List<OperationTask> tasks = new ArrayList<>();
        processTasks(request, tasks::add);
        if (tasks.isEmpty()) {
            listener.onResponse(new NodeTasksResponse(clusterService.localNode().getId(), emptyList(), emptyList()));
            return;
        }
        AtomicArray<Tuple<TaskResponse, Exception>> responses = new AtomicArray<>(tasks.size());
        final AtomicInteger counter = new AtomicInteger(tasks.size());
        for (int i = 0; i < tasks.size(); i++) {
            final int taskIndex = i;
            ActionListener<TaskResponse> taskListener = new ActionListener<TaskResponse>() {
                @Override
                public void onResponse(TaskResponse response) {
                    responses.setOnce(taskIndex, response == null ? null : new Tuple<>(response, null));
                    respondIfFinished();
                }

                @Override
                public void onFailure(Exception e) {
                    responses.setOnce(taskIndex, new Tuple<>(null, e));
                    respondIfFinished();
                }

                private void respondIfFinished() {
                    if (counter.decrementAndGet() != 0) {
                        return;
                    }
                    List<TaskResponse> results = new ArrayList<>();
                    List<TaskOperationFailure> exceptions = new ArrayList<>();
                    for (Tuple<TaskResponse, Exception> response : responses.asList()) {
                        if (response.v1() == null) {
                            assert response.v2() != null;
                            exceptions.add(
                                new TaskOperationFailure(clusterService.localNode().getId(), tasks.get(taskIndex).getId(), response.v2())
                            );
                        } else {
                            assert response.v2() == null;
                            results.add(response.v1());
                        }
                    }
                    listener.onResponse(new NodeTasksResponse(clusterService.localNode().getId(), results, exceptions));
                }
            };
            try {
                taskOperation(request, tasks.get(taskIndex), taskListener);
            } catch (Exception e) {
                taskListener.onFailure(e);
            }
        }
    }

    protected String[] filterNodeIds(DiscoveryNodes nodes, String[] nodesIds) {
        return nodesIds;
    }

    protected String[] resolveNodes(TasksRequest request, ClusterState clusterState) {
        if (request.getTaskId().isSet()) {
            return new String[] { request.getTaskId().getNodeId() };
        } else {
            return clusterState.nodes().resolveNodes(request.getNodes());
        }
    }

    protected void processTasks(TasksRequest request, Consumer<OperationTask> operation) {
        if (request.getTaskId().isSet()) {
            // we are only checking one task, we can optimize it
            Task task = taskManager.getTask(request.getTaskId().getId());
            if (task != null) {
                if (request.match(task)) {
                    operation.accept((OperationTask) task);
                } else {
                    throw new ResourceNotFoundException("task [{}] doesn't support this operation", request.getTaskId());
                }
            } else {
                throw new ResourceNotFoundException("task [{}] is missing", request.getTaskId());
            }
        } else {
            for (Task task : taskManager.getTasks().values()) {
                if (request.match(task)) {
                    operation.accept((OperationTask) task);
                }
            }
        }
    }

    protected abstract TasksResponse newResponse(
        TasksRequest request,
        List<TaskResponse> tasks,
        List<TaskOperationFailure> taskOperationFailures,
        List<FailedNodeException> failedNodeExceptions
    );

    @SuppressWarnings("unchecked")
    protected TasksResponse newResponse(TasksRequest request, AtomicReferenceArray responses) {
        List<TaskResponse> tasks = new ArrayList<>();
        List<FailedNodeException> failedNodeExceptions = new ArrayList<>();
        List<TaskOperationFailure> taskOperationFailures = new ArrayList<>();
        for (int i = 0; i < responses.length(); i++) {
            Object response = responses.get(i);
            if (response instanceof FailedNodeException) {
                failedNodeExceptions.add((FailedNodeException) response);
            } else {
                NodeTasksResponse tasksResponse = (NodeTasksResponse) response;
                if (tasksResponse.results != null) {
                    tasks.addAll(tasksResponse.results);
                }
                if (tasksResponse.exceptions != null) {
                    taskOperationFailures.addAll(tasksResponse.exceptions);
                }
            }
        }
        return newResponse(request, tasks, taskOperationFailures, failedNodeExceptions);
    }

    /**
     * Perform the required operation on the task. It is OK start an asynchronous operation or to throw an exception but not both.
     */
    protected abstract void taskOperation(TasksRequest request, OperationTask task, ActionListener<TaskResponse> listener);

    /**
     * Asynchronous single action
     *
     * @density.internal
     */
    private class AsyncAction {

        private final TasksRequest request;
        private final String[] nodesIds;
        private final DiscoveryNode[] nodes;
        private final ActionListener<TasksResponse> listener;
        private final AtomicReferenceArray<Object> responses;
        private final AtomicInteger counter = new AtomicInteger();
        private final Task task;

        private AsyncAction(Task task, TasksRequest request, ActionListener<TasksResponse> listener) {
            this.task = task;
            this.request = request;
            this.listener = listener;
            ClusterState clusterState = clusterService.state();
            String[] nodesIds = resolveNodes(request, clusterState);
            this.nodesIds = filterNodeIds(clusterState.nodes(), nodesIds);
            final Map<String, DiscoveryNode> nodes = clusterState.nodes().getNodes();
            this.nodes = new DiscoveryNode[nodesIds.length];
            for (int i = 0; i < this.nodesIds.length; i++) {
                this.nodes[i] = nodes.get(this.nodesIds[i]);
            }
            this.responses = new AtomicReferenceArray<>(this.nodesIds.length);
        }

        private void start() {
            if (nodesIds.length == 0) {
                // nothing to do
                try {
                    listener.onResponse(newResponse(request, responses));
                } catch (Exception e) {
                    logger.debug("failed to generate empty response", e);
                    listener.onFailure(e);
                }
            } else {
                TransportRequestOptions.Builder builder = TransportRequestOptions.builder();
                if (request.getTimeout() != null) {
                    builder.withTimeout(request.getTimeout());
                }
                for (int i = 0; i < nodesIds.length; i++) {
                    final String nodeId = nodesIds[i];
                    final int idx = i;
                    final DiscoveryNode node = nodes[i];
                    try {
                        if (node == null) {
                            onFailure(idx, nodeId, new NoSuchNodeException(nodeId));
                        } else {
                            NodeTaskRequest nodeRequest = new NodeTaskRequest(request);
                            nodeRequest.setParentTask(clusterService.localNode().getId(), task.getId());
                            transportService.sendRequest(
                                node,
                                transportNodeAction,
                                nodeRequest,
                                builder.build(),
                                new TransportResponseHandler<NodeTasksResponse>() {
                                    @Override
                                    public NodeTasksResponse read(StreamInput in) throws IOException {
                                        return new NodeTasksResponse(in);
                                    }

                                    @Override
                                    public void handleResponse(NodeTasksResponse response) {
                                        onOperation(idx, response);
                                    }

                                    @Override
                                    public void handleException(TransportException exp) {
                                        onFailure(idx, node.getId(), exp);
                                    }

                                    @Override
                                    public String executor() {
                                        return ThreadPool.Names.SAME;
                                    }
                                }
                            );
                        }
                    } catch (Exception e) {
                        onFailure(idx, nodeId, e);
                    }
                }
            }
        }

        private void onOperation(int idx, NodeTasksResponse nodeResponse) {
            responses.set(idx, nodeResponse);
            if (counter.incrementAndGet() == responses.length()) {
                finishHim();
            }
        }

        private void onFailure(int idx, String nodeId, Throwable t) {
            if (logger.isDebugEnabled() && !(t instanceof NodeShouldNotConnectException)) {
                logger.debug(new ParameterizedMessage("failed to execute on node [{}]", nodeId), t);
            }

            responses.set(idx, new FailedNodeException(nodeId, "Failed node [" + nodeId + "]", t));

            if (counter.incrementAndGet() == responses.length()) {
                finishHim();
            }
        }

        private void finishHim() {
            TasksResponse finalResponse;
            try {
                finalResponse = newResponse(request, responses);
            } catch (Exception e) {
                logger.debug("failed to combine responses from nodes", e);
                listener.onFailure(e);
                return;
            }
            listener.onResponse(finalResponse);
        }
    }

    /**
     * Node level transport handler
     *
     * @density.internal
     */
    class NodeTransportHandler implements TransportRequestHandler<NodeTaskRequest> {

        @Override
        public void messageReceived(final NodeTaskRequest request, final TransportChannel channel, Task task) throws Exception {
            nodeOperation(request, ActionListener.wrap(channel::sendResponse, e -> {
                try {
                    channel.sendResponse(e);
                } catch (IOException e1) {
                    e1.addSuppressed(e);
                    logger.warn("Failed to send failure", e1);
                }
            }));
        }
    }

    /**
     * Node level task request
     *
     * @density.internal
     */
    private class NodeTaskRequest extends TransportRequest {
        private TasksRequest tasksRequest;

        protected NodeTaskRequest(StreamInput in) throws IOException {
            super(in);
            this.tasksRequest = requestReader.read(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            tasksRequest.writeTo(out);
        }

        protected NodeTaskRequest(TasksRequest tasksRequest) {
            super();
            this.tasksRequest = tasksRequest;
        }

    }

    /**
     * Node level task response
     *
     * @density.internal
     */
    private class NodeTasksResponse extends TransportResponse {
        protected String nodeId;
        protected List<TaskOperationFailure> exceptions;
        protected List<TaskResponse> results;

        NodeTasksResponse(StreamInput in) throws IOException {
            super(in);
            nodeId = in.readString();
            int resultsSize = in.readVInt();
            results = new ArrayList<>(resultsSize);
            for (; resultsSize > 0; resultsSize--) {
                final TaskResponse result = in.readBoolean() ? responseReader.read(in) : null;
                results.add(result);
            }
            if (in.readBoolean()) {
                int taskFailures = in.readVInt();
                exceptions = new ArrayList<>(taskFailures);
                for (int i = 0; i < taskFailures; i++) {
                    exceptions.add(new TaskOperationFailure(in));
                }
            } else {
                exceptions = null;
            }
        }

        NodeTasksResponse(String nodeId, List<TaskResponse> results, List<TaskOperationFailure> exceptions) {
            this.nodeId = nodeId;
            this.results = results;
            this.exceptions = exceptions;
        }

        public String getNodeId() {
            return nodeId;
        }

        public List<TaskOperationFailure> getExceptions() {
            return exceptions;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(nodeId);
            out.writeVInt(results.size());
            for (TaskResponse result : results) {
                if (result != null) {
                    out.writeBoolean(true);
                    result.writeTo(out);
                } else {
                    out.writeBoolean(false);
                }
            }
            out.writeBoolean(exceptions != null);
            if (exceptions != null) {
                int taskFailures = exceptions.size();
                out.writeVInt(taskFailures);
                for (TaskOperationFailure exception : exceptions) {
                    exception.writeTo(out);
                }
            }
        }
    }
}
