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

package org.density.action.get;

import org.density.Version;
import org.density.action.IndicesRequest;
import org.density.action.RoutingMissingException;
import org.density.action.support.ActionFilters;
import org.density.cluster.ClusterName;
import org.density.cluster.ClusterState;
import org.density.cluster.metadata.IndexMetadata;
import org.density.cluster.metadata.IndexNameExpressionResolver;
import org.density.cluster.metadata.Metadata;
import org.density.cluster.node.DiscoveryNode;
import org.density.cluster.routing.OperationRouting;
import org.density.cluster.routing.Preference;
import org.density.cluster.routing.ShardIterator;
import org.density.cluster.service.ClusterService;
import org.density.common.settings.Settings;
import org.density.common.util.concurrent.AtomicArray;
import org.density.common.util.concurrent.ThreadContext;
import org.density.common.xcontent.XContentFactory;
import org.density.common.xcontent.XContentHelper;
import org.density.core.action.ActionListener;
import org.density.core.common.bytes.BytesReference;
import org.density.core.index.Index;
import org.density.core.index.shard.ShardId;
import org.density.core.tasks.TaskId;
import org.density.core.xcontent.MediaTypeRegistry;
import org.density.indices.IndicesService;
import org.density.indices.replication.common.ReplicationType;
import org.density.tasks.Task;
import org.density.tasks.TaskManager;
import org.density.telemetry.tracing.noop.NoopTracer;
import org.density.test.DensityTestCase;
import org.density.threadpool.TestThreadPool;
import org.density.threadpool.ThreadPool;
import org.density.transport.Transport;
import org.density.transport.TransportService;
import org.density.transport.client.node.NodeClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.density.common.UUIDs.randomBase64UUID;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportMultiGetActionTests extends DensityTestCase {

    private static ThreadPool threadPool;
    private static TransportService transportService;
    private static ClusterService clusterService;
    private static TransportMultiGetAction transportAction;
    private static TransportShardMultiGetAction shardAction;

    private static ClusterState clusterState(ReplicationType replicationType, Index index1, Index index2) throws IOException {
        return ClusterState.builder(new ClusterName(TransportMultiGetActionTests.class.getSimpleName()))
            .metadata(
                new Metadata.Builder().put(
                    new IndexMetadata.Builder(index1.getName()).settings(
                        Settings.builder()
                            .put("index.version.created", Version.CURRENT)
                            .put("index.number_of_shards", 1)
                            .put("index.number_of_replicas", 1)
                            .put(IndexMetadata.SETTING_REPLICATION_TYPE, replicationType)
                            .put(IndexMetadata.SETTING_INDEX_UUID, index1.getUUID())
                    )
                        .putMapping(
                            XContentHelper.convertToJson(
                                BytesReference.bytes(
                                    XContentFactory.jsonBuilder()
                                        .startObject()
                                        .startObject("_doc")
                                        .startObject("_routing")
                                        .field("required", false)
                                        .endObject()
                                        .endObject()
                                        .endObject()
                                ),
                                true,
                                MediaTypeRegistry.JSON
                            )
                        )
                )
                    .put(
                        new IndexMetadata.Builder(index2.getName()).settings(
                            Settings.builder()
                                .put("index.version.created", Version.CURRENT)
                                .put("index.number_of_shards", 1)
                                .put("index.number_of_replicas", 1)
                                .put(IndexMetadata.SETTING_REPLICATION_TYPE, replicationType)
                                .put(IndexMetadata.SETTING_INDEX_UUID, index1.getUUID())
                        )
                            .putMapping(
                                XContentHelper.convertToJson(
                                    BytesReference.bytes(
                                        XContentFactory.jsonBuilder()
                                            .startObject()
                                            .startObject("_doc")
                                            .startObject("_routing")
                                            .field("required", true)
                                            .endObject()
                                            .endObject()
                                            .endObject()
                                    ),
                                    true,
                                    MediaTypeRegistry.JSON
                                )
                            )
                    )
            )
            .build();
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        threadPool = new TestThreadPool(TransportMultiGetActionTests.class.getSimpleName());

        transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundAddress -> DiscoveryNode.createLocal(
                Settings.builder().put("node.name", "node1").build(),
                boundAddress.publishAddress(),
                randomBase64UUID()
            ),
            null,
            emptySet(),
            NoopTracer.INSTANCE
        ) {
            @Override
            public TaskManager getTaskManager() {
                return taskManager;
            }
        };

        final Index index1 = new Index("index1", randomBase64UUID());
        final Index index2 = new Index("index2", randomBase64UUID());
        ClusterState clusterState = clusterState(randomBoolean() ? ReplicationType.SEGMENT : ReplicationType.DOCUMENT, index1, index2);

        final ShardIterator index1ShardIterator = mock(ShardIterator.class);
        when(index1ShardIterator.shardId()).thenReturn(new ShardId(index1, randomInt()));

        final ShardIterator index2ShardIterator = mock(ShardIterator.class);
        when(index2ShardIterator.shardId()).thenReturn(new ShardId(index2, randomInt()));

        final OperationRouting operationRouting = mock(OperationRouting.class);
        when(
            operationRouting.getShards(eq(clusterState), eq(index1.getName()), anyString(), nullable(String.class), nullable(String.class))
        ).thenReturn(index1ShardIterator);
        when(operationRouting.shardId(eq(clusterState), eq(index1.getName()), nullable(String.class), nullable(String.class))).thenReturn(
            new ShardId(index1, randomInt())
        );
        when(
            operationRouting.getShards(eq(clusterState), eq(index2.getName()), anyString(), nullable(String.class), nullable(String.class))
        ).thenReturn(index2ShardIterator);
        when(operationRouting.shardId(eq(clusterState), eq(index2.getName()), nullable(String.class), nullable(String.class))).thenReturn(
            new ShardId(index2, randomInt())
        );

        clusterService = mock(ClusterService.class);
        when(clusterService.localNode()).thenReturn(transportService.getLocalNode());
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterService.operationRouting()).thenReturn(operationRouting);

        shardAction = new TransportShardMultiGetAction(
            clusterService,
            transportService,
            mock(IndicesService.class),
            threadPool,
            new ActionFilters(emptySet()),
            new Resolver()
        ) {
            @Override
            protected void doExecute(Task task, MultiGetShardRequest request, ActionListener<MultiGetShardResponse> listener) {}
        };
    }

    @AfterClass
    public static void afterClass() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
        transportService = null;
        clusterService = null;
        transportAction = null;
        shardAction = null;
    }

    public void testTransportMultiGetAction() {
        final Task task = createTask();
        final NodeClient client = new NodeClient(Settings.EMPTY, threadPool);
        final MultiGetRequestBuilder request = new MultiGetRequestBuilder(client, MultiGetAction.INSTANCE);
        request.add(new MultiGetRequest.Item("index1", "1"));
        request.add(new MultiGetRequest.Item("index1", "2"));

        final AtomicBoolean shardActionInvoked = new AtomicBoolean(false);
        transportAction = new TransportMultiGetAction(
            transportService,
            clusterService,
            shardAction,
            new ActionFilters(emptySet()),
            new Resolver()
        ) {
            @Override
            protected void executeShardAction(
                final ActionListener<MultiGetResponse> listener,
                final AtomicArray<MultiGetItemResponse> responses,
                final Map<ShardId, MultiGetShardRequest> shardRequests
            ) {
                shardActionInvoked.set(true);
                assertEquals(2, responses.length());
                assertNull(responses.get(0));
                assertNull(responses.get(1));
            }
        };

        transportAction.execute(task, request.request(), new ActionListenerAdapter());
        assertTrue(shardActionInvoked.get());
    }

    public void testTransportMultiGetAction_withMissingRouting() {
        final Task task = createTask();
        final NodeClient client = new NodeClient(Settings.EMPTY, threadPool);
        final MultiGetRequestBuilder request = new MultiGetRequestBuilder(client, MultiGetAction.INSTANCE);
        request.add(new MultiGetRequest.Item("index2", "1").routing("1"));
        request.add(new MultiGetRequest.Item("index2", "2"));

        final AtomicBoolean shardActionInvoked = new AtomicBoolean(false);
        transportAction = new TransportMultiGetAction(
            transportService,
            clusterService,
            shardAction,
            new ActionFilters(emptySet()),
            new Resolver()
        ) {
            @Override
            protected void executeShardAction(
                final ActionListener<MultiGetResponse> listener,
                final AtomicArray<MultiGetItemResponse> responses,
                final Map<ShardId, MultiGetShardRequest> shardRequests
            ) {
                shardActionInvoked.set(true);
                assertEquals(2, responses.length());
                assertNull(responses.get(0));
                assertThat(responses.get(1).getFailure().getFailure(), instanceOf(RoutingMissingException.class));
                assertThat(responses.get(1).getFailure().getFailure().getMessage(), equalTo("routing is required for [index2]/[_doc]/[2]"));
            }
        };

        transportAction.execute(task, request.request(), new ActionListenerAdapter());
        assertTrue(shardActionInvoked.get());

    }

    public void testShouldForcePrimaryRouting() throws IOException {
        final Index index1 = new Index("index1", randomBase64UUID());
        final Index index2 = new Index("index2", randomBase64UUID());
        Metadata metadata = clusterState(ReplicationType.SEGMENT, index1, index2).getMetadata();

        // should return false since preference is set for request
        assertFalse(TransportMultiGetAction.shouldForcePrimaryRouting(metadata, true, Preference.REPLICA.type(), "index1"));

        // should return false since request is not realtime
        assertFalse(TransportMultiGetAction.shouldForcePrimaryRouting(metadata, false, null, "index2"));

        // should return true since segment replication is enabled
        assertTrue(TransportMultiGetAction.shouldForcePrimaryRouting(metadata, true, null, "index1"));

        // should return false since index doesn't exist
        assertFalse(TransportMultiGetAction.shouldForcePrimaryRouting(metadata, true, null, "index3"));

        metadata = clusterState(ReplicationType.DOCUMENT, index1, index2).getMetadata();

        // should fail since document replication enabled
        assertFalse(TransportGetAction.shouldForcePrimaryRouting(metadata, true, null, "index1"));

    }

    private static Task createTask() {
        return new Task(
            randomLong(),
            "transport",
            MultiGetAction.NAME,
            "description",
            new TaskId(randomLong() + ":" + randomLong()),
            emptyMap()
        );
    }

    static class Resolver extends IndexNameExpressionResolver {

        Resolver() {
            super(new ThreadContext(Settings.EMPTY));
        }

        @Override
        public Index concreteSingleIndex(ClusterState state, IndicesRequest request) {
            return new Index("index1", randomBase64UUID());
        }
    }

    static class ActionListenerAdapter implements ActionListener<MultiGetResponse> {

        @Override
        public void onResponse(MultiGetResponse response) {}

        @Override
        public void onFailure(Exception e) {}
    }
}
