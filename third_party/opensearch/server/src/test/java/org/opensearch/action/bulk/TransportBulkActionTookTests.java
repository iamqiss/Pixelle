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

package org.density.action.bulk;

import org.apache.lucene.util.Constants;
import org.density.Version;
import org.density.action.ActionRequest;
import org.density.action.ActionType;
import org.density.action.IndicesRequest;
import org.density.action.admin.indices.create.CreateIndexResponse;
import org.density.action.support.ActionFilters;
import org.density.action.support.AutoCreateIndex;
import org.density.cluster.ClusterState;
import org.density.cluster.metadata.IndexNameExpressionResolver;
import org.density.cluster.node.DiscoveryNode;
import org.density.cluster.node.DiscoveryNodeRole;
import org.density.cluster.service.ClusterService;
import org.density.common.settings.Settings;
import org.density.common.util.concurrent.AtomicArray;
import org.density.common.util.concurrent.ThreadContext;
import org.density.core.action.ActionListener;
import org.density.core.action.ActionResponse;
import org.density.core.common.Strings;
import org.density.core.xcontent.MediaTypeRegistry;
import org.density.index.IndexNotFoundException;
import org.density.index.IndexingPressureService;
import org.density.indices.SystemIndices;
import org.density.tasks.Task;
import org.density.telemetry.tracing.noop.NoopTracer;
import org.density.test.DensityTestCase;
import org.density.test.VersionUtils;
import org.density.test.transport.CapturingTransport;
import org.density.threadpool.TestThreadPool;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportService;
import org.density.transport.client.node.NodeClient;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import static java.util.Collections.emptyMap;
import static org.density.test.ClusterServiceUtils.createClusterService;
import static org.density.test.StreamsUtils.copyToStringFromClasspath;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class TransportBulkActionTookTests extends DensityTestCase {

    private static ThreadPool threadPool;
    private ClusterService clusterService;

    @BeforeClass
    public static void beforeClass() {
        threadPool = new TestThreadPool("TransportBulkActionTookTests");
    }

    @AfterClass
    public static void afterClass() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        DiscoveryNode discoveryNode = new DiscoveryNode(
            "node",
            DensityTestCase.buildNewFakeTransportAddress(),
            emptyMap(),
            DiscoveryNodeRole.BUILT_IN_ROLES,
            VersionUtils.randomCompatibleVersion(random(), Version.CURRENT)
        );
        clusterService = createClusterService(threadPool, discoveryNode);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        clusterService.close();
    }

    private TransportBulkAction createAction(boolean controlled, AtomicLong expected) {
        CapturingTransport capturingTransport = new CapturingTransport();
        TransportService transportService = capturingTransport.createTransportService(
            clusterService.getSettings(),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundAddress -> clusterService.localNode(),
            null,
            Collections.emptySet(),
            NoopTracer.INSTANCE
        );
        transportService.start();
        transportService.acceptIncomingRequests();
        IndexNameExpressionResolver resolver = new Resolver();
        ActionFilters actionFilters = new ActionFilters(new HashSet<>());

        NodeClient client = new NodeClient(Settings.EMPTY, threadPool) {
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                listener.onResponse((Response) new CreateIndexResponse(false, false, null));
            }
        };

        if (controlled) {

            return new TestTransportBulkAction(
                threadPool,
                transportService,
                clusterService,
                null,
                client,
                actionFilters,
                resolver,
                null,
                expected::get
            ) {

                @Override
                void executeBulk(
                    Task task,
                    BulkRequest bulkRequest,
                    long startTimeNanos,
                    ActionListener<BulkResponse> listener,
                    AtomicArray<BulkItemResponse> responses,
                    Map<String, IndexNotFoundException> indicesThatCannotBeCreated
                ) {
                    expected.set(1000000);
                    super.executeBulk(task, bulkRequest, startTimeNanos, listener, responses, indicesThatCannotBeCreated);
                }
            };
        } else {
            return new TestTransportBulkAction(
                threadPool,
                transportService,
                clusterService,
                null,
                client,
                actionFilters,
                resolver,
                null,
                System::nanoTime
            ) {

                @Override
                void executeBulk(
                    Task task,
                    BulkRequest bulkRequest,
                    long startTimeNanos,
                    ActionListener<BulkResponse> listener,
                    AtomicArray<BulkItemResponse> responses,
                    Map<String, IndexNotFoundException> indicesThatCannotBeCreated
                ) {
                    long elapsed = spinForAtLeastOneMillisecond();
                    expected.set(elapsed);
                    super.executeBulk(task, bulkRequest, startTimeNanos, listener, responses, indicesThatCannotBeCreated);
                }
            };
        }
    }

    // test unit conversion with a controlled clock
    public void testTookWithControlledClock() throws Exception {
        runTestTook(true);
    }

    // test took advances with System#nanoTime
    public void testTookWithRealClock() throws Exception {
        runTestTook(false);
    }

    private void runTestTook(boolean controlled) throws Exception {
        String bulkAction = copyToStringFromClasspath("/org/density/action/bulk/simple-bulk.json");
        // translate Windows line endings (\r\n) to standard ones (\n)
        if (Constants.WINDOWS) {
            bulkAction = Strings.replace(bulkAction, "\r\n", "\n");
        }
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(bulkAction.getBytes(StandardCharsets.UTF_8), 0, bulkAction.length(), null, MediaTypeRegistry.JSON);
        AtomicLong expected = new AtomicLong();
        TransportBulkAction action = createAction(controlled, expected);
        action.doExecute(null, bulkRequest, new ActionListener<BulkResponse>() {
            @Override
            public void onResponse(BulkResponse bulkItemResponses) {
                if (controlled) {
                    assertThat(
                        bulkItemResponses.getTook().getMillis(),
                        equalTo(TimeUnit.MILLISECONDS.convert(expected.get(), TimeUnit.NANOSECONDS))
                    );
                } else {
                    assertThat(
                        bulkItemResponses.getTook().getMillis(),
                        greaterThanOrEqualTo(TimeUnit.MILLISECONDS.convert(expected.get(), TimeUnit.NANOSECONDS))
                    );
                }
            }

            @Override
            public void onFailure(Exception e) {

            }
        });
    }

    static class Resolver extends IndexNameExpressionResolver {
        Resolver() {
            super(new ThreadContext(Settings.EMPTY));
        }

        @Override
        public String[] concreteIndexNames(ClusterState state, IndicesRequest request) {
            return request.indices();
        }
    }

    static class TestTransportBulkAction extends TransportBulkAction {

        TestTransportBulkAction(
            ThreadPool threadPool,
            TransportService transportService,
            ClusterService clusterService,
            TransportShardBulkAction shardBulkAction,
            NodeClient client,
            ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver,
            AutoCreateIndex autoCreateIndex,
            LongSupplier relativeTimeProvider
        ) {
            super(
                threadPool,
                transportService,
                clusterService,
                null,
                shardBulkAction,
                client,
                actionFilters,
                indexNameExpressionResolver,
                autoCreateIndex,
                new IndexingPressureService(Settings.EMPTY, clusterService),
                null,
                new SystemIndices(emptyMap()),
                relativeTimeProvider,
                NoopTracer.INSTANCE
            );
        }

        @Override
        boolean needToCheck() {
            return randomBoolean();
        }

        @Override
        boolean shouldAutoCreate(String index, ClusterState state) {
            return randomBoolean();
        }

    }
}
