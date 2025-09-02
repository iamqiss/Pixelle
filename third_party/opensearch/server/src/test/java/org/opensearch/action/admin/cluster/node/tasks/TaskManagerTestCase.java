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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.density.action.admin.cluster.node.tasks;

import org.density.Version;
import org.density.action.FailedNodeException;
import org.density.action.admin.cluster.node.tasks.cancel.TransportCancelTasksAction;
import org.density.action.admin.cluster.node.tasks.get.TransportGetTaskAction;
import org.density.action.admin.cluster.node.tasks.list.TransportListTasksAction;
import org.density.action.support.ActionFilters;
import org.density.action.support.nodes.BaseNodeResponse;
import org.density.action.support.nodes.BaseNodesRequest;
import org.density.action.support.nodes.BaseNodesResponse;
import org.density.action.support.nodes.TransportNodesAction;
import org.density.action.support.replication.ClusterStateCreationUtils;
import org.density.cluster.ClusterModule;
import org.density.cluster.ClusterName;
import org.density.cluster.node.DiscoveryNode;
import org.density.cluster.service.ClusterService;
import org.density.common.SetOnce;
import org.density.common.lease.Releasable;
import org.density.common.network.NetworkService;
import org.density.common.settings.ClusterSettings;
import org.density.common.settings.Settings;
import org.density.common.util.PageCacheRecycler;
import org.density.core.common.io.stream.NamedWriteableRegistry;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;
import org.density.core.common.io.stream.Writeable;
import org.density.core.common.transport.BoundTransportAddress;
import org.density.core.indices.breaker.NoneCircuitBreakerService;
import org.density.core.xcontent.NamedXContentRegistry;
import org.density.tasks.TaskCancellationService;
import org.density.tasks.TaskManager;
import org.density.tasks.TaskResourceTrackingService;
import org.density.telemetry.tracing.noop.NoopTracer;
import org.density.test.DensityTestCase;
import org.density.test.tasks.MockTaskManager;
import org.density.threadpool.RunnableTaskExecutionListener;
import org.density.threadpool.TestThreadPool;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportRequest;
import org.density.transport.TransportService;
import org.density.transport.client.Client;
import org.density.transport.nio.MockNioTransport;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.density.test.ClusterServiceUtils.createClusterService;
import static org.density.test.ClusterServiceUtils.setState;
import static org.mockito.Mockito.mock;

/**
 * The test case for unit testing task manager and related transport actions
 */
public abstract class TaskManagerTestCase extends DensityTestCase {

    protected ThreadPool threadPool;
    protected TestNode[] testNodes;
    protected int nodesCount;
    protected AtomicReference<RunnableTaskExecutionListener> runnableTaskListener;

    @Before
    public void setupThreadPool() {
        runnableTaskListener = new AtomicReference<>();
        threadPool = new TestThreadPool(TransportTasksActionTests.class.getSimpleName(), runnableTaskListener);
    }

    public void setupTestNodes(Settings settings) {
        nodesCount = randomIntBetween(2, 10);
        testNodes = new TestNode[nodesCount];
        for (int i = 0; i < testNodes.length; i++) {
            testNodes[i] = new TestNode("node" + i, threadPool, settings);
        }
    }

    @After
    public final void shutdownTestNodes() throws Exception {
        if (testNodes != null) {
            for (TestNode testNode : testNodes) {
                testNode.close();
            }
            testNodes = null;
        }
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
    }

    static class NodeResponse extends BaseNodeResponse {

        protected NodeResponse(StreamInput in) throws IOException {
            super(in);
        }

        protected NodeResponse(DiscoveryNode node) {
            super(node);
        }
    }

    static class NodesResponse extends BaseNodesResponse<NodeResponse> {

        protected NodesResponse(ClusterName clusterName, List<NodeResponse> nodes, List<FailedNodeException> failures) {
            super(clusterName, nodes, failures);
        }

        @Override
        protected List<NodeResponse> readNodesFrom(StreamInput in) throws IOException {
            return in.readList(NodeResponse::new);
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<NodeResponse> nodes) throws IOException {
            out.writeList(nodes);
        }

        public int failureCount() {
            return failures().size();
        }
    }

    /**
     * Simulates node-based task that can be used to block node tasks so they are guaranteed to be registered by task manager
     */
    abstract class AbstractTestNodesAction<NodesRequest extends BaseNodesRequest<NodesRequest>, NodeRequest extends TransportRequest>
        extends TransportNodesAction<NodesRequest, NodesResponse, NodeRequest, NodeResponse> {

        AbstractTestNodesAction(
            String actionName,
            ThreadPool threadPool,
            ClusterService clusterService,
            TransportService transportService,
            Writeable.Reader<NodesRequest> request,
            Writeable.Reader<NodeRequest> nodeRequest
        ) {
            super(
                actionName,
                threadPool,
                clusterService,
                transportService,
                new ActionFilters(new HashSet<>()),
                request,
                nodeRequest,
                ThreadPool.Names.GENERIC,
                NodeResponse.class
            );
        }

        @Override
        protected NodesResponse newResponse(NodesRequest request, List<NodeResponse> responses, List<FailedNodeException> failures) {
            return new NodesResponse(clusterService.getClusterName(), responses, failures);
        }

        @Override
        protected NodeResponse newNodeResponse(StreamInput in) throws IOException {
            return new NodeResponse(in);
        }

        @Override
        protected abstract NodeResponse nodeOperation(NodeRequest request);

    }

    public static class TestNode implements Releasable {
        public TestNode(String name, ThreadPool threadPool, Settings settings) {
            final Function<BoundTransportAddress, DiscoveryNode> boundTransportAddressDiscoveryNodeFunction = address -> {
                discoveryNode.set(new DiscoveryNode(name, address.publishAddress(), emptyMap(), emptySet(), Version.CURRENT));
                return discoveryNode.get();
            };
            transportService = new TransportService(
                settings,
                new MockNioTransport(
                    settings,
                    Version.CURRENT,
                    threadPool,
                    new NetworkService(Collections.emptyList()),
                    PageCacheRecycler.NON_RECYCLING_INSTANCE,
                    new NamedWriteableRegistry(ClusterModule.getNamedWriteables()),
                    new NoneCircuitBreakerService(),
                    NoopTracer.INSTANCE
                ),
                threadPool,
                TransportService.NOOP_TRANSPORT_INTERCEPTOR,
                boundTransportAddressDiscoveryNodeFunction,
                null,
                Collections.emptySet(),
                NoopTracer.INSTANCE
            ) {
                @Override
                protected TaskManager createTaskManager(
                    Settings settings,
                    ClusterSettings clusterSettings,
                    ThreadPool threadPool,
                    Set<String> taskHeaders
                ) {
                    if (MockTaskManager.USE_MOCK_TASK_MANAGER_SETTING.get(settings)) {
                        return new MockTaskManager(settings, threadPool, taskHeaders);
                    } else {
                        return super.createTaskManager(settings, clusterSettings, threadPool, taskHeaders);
                    }
                }
            };
            transportService.getTaskManager().setTaskCancellationService(new TaskCancellationService(transportService));
            transportService.start();
            clusterService = createClusterService(threadPool, discoveryNode.get());
            clusterService.addStateApplier(transportService.getTaskManager());
            taskResourceTrackingService = new TaskResourceTrackingService(settings, clusterService.getClusterSettings(), threadPool);
            transportService.getTaskManager().setTaskResourceTrackingService(taskResourceTrackingService);
            ActionFilters actionFilters = new ActionFilters(emptySet());
            transportListTasksAction = new TransportListTasksAction(
                clusterService,
                transportService,
                actionFilters,
                taskResourceTrackingService
            );
            transportCancelTasksAction = new TransportCancelTasksAction(clusterService, transportService, actionFilters);
            Client mockClient = mock(Client.class);
            NamedXContentRegistry namedXContentRegistry = mock(NamedXContentRegistry.class);
            transportGetTaskAction = new TransportGetTaskAction(
                threadPool,
                transportService,
                actionFilters,
                clusterService,
                mockClient,
                namedXContentRegistry,
                taskResourceTrackingService
            );
            transportService.acceptIncomingRequests();
        }

        public final ClusterService clusterService;
        public final TransportService transportService;
        public final TaskResourceTrackingService taskResourceTrackingService;
        private final SetOnce<DiscoveryNode> discoveryNode = new SetOnce<>();
        public final TransportListTasksAction transportListTasksAction;
        public final TransportCancelTasksAction transportCancelTasksAction;
        public final TransportGetTaskAction transportGetTaskAction;

        @Override
        public void close() {
            clusterService.close();
            transportService.close();
        }

        public String getNodeId() {
            return discoveryNode().getId();
        }

        public DiscoveryNode discoveryNode() {
            return discoveryNode.get();
        }
    }

    public static void connectNodes(TestNode... nodes) {
        DiscoveryNode[] discoveryNodes = new DiscoveryNode[nodes.length];
        for (int i = 0; i < nodes.length; i++) {
            discoveryNodes[i] = nodes[i].discoveryNode();
        }
        DiscoveryNode clusterManager = discoveryNodes[0];
        for (TestNode node : nodes) {
            setState(node.clusterService, ClusterStateCreationUtils.state(node.discoveryNode(), clusterManager, discoveryNodes));
        }
        for (TestNode nodeA : nodes) {
            for (TestNode nodeB : nodes) {
                nodeA.transportService.connectToNode(nodeB.discoveryNode());
            }
        }
    }

    public static RecordingTaskManagerListener[] setupListeners(TestNode[] nodes, String... actionMasks) {
        RecordingTaskManagerListener[] listeners = new RecordingTaskManagerListener[nodes.length];
        for (int i = 0; i < nodes.length; i++) {
            listeners[i] = new RecordingTaskManagerListener(nodes[i].getNodeId(), actionMasks);
            ((MockTaskManager) (nodes[i].transportService.getTaskManager())).addListener(listeners[i]);
        }
        return listeners;
    }
}
