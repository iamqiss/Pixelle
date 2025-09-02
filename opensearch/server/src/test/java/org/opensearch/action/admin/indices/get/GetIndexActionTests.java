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

package org.density.action.admin.indices.get;

import org.density.action.IndicesRequest;
import org.density.action.support.ActionFilters;
import org.density.action.support.replication.ClusterStateCreationUtils;
import org.density.cluster.ClusterState;
import org.density.cluster.metadata.Context;
import org.density.cluster.metadata.IndexNameExpressionResolver;
import org.density.cluster.service.ClusterService;
import org.density.common.settings.IndexScopedSettings;
import org.density.common.settings.Settings;
import org.density.common.settings.SettingsFilter;
import org.density.common.settings.SettingsModule;
import org.density.common.util.concurrent.ThreadContext;
import org.density.core.action.ActionListener;
import org.density.core.index.Index;
import org.density.indices.IndicesService;
import org.density.telemetry.tracing.noop.NoopTracer;
import org.density.test.DensitySingleNodeTestCase;
import org.density.test.transport.CapturingTransport;
import org.density.threadpool.TestThreadPool;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportService;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;

public class GetIndexActionTests extends DensitySingleNodeTestCase {

    private TransportService transportService;
    private ClusterService clusterService;
    private IndicesService indicesService;
    private ThreadPool threadPool;
    private SettingsFilter settingsFilter;
    private final String indexName = "test_index";
    private Context context;
    private TestTransportGetIndexAction getIndexAction;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        settingsFilter = new SettingsModule(Settings.EMPTY, emptyList(), emptyList(), emptySet()).getSettingsFilter();
        threadPool = new TestThreadPool("GetIndexActionTests");
        clusterService = getInstanceFromNode(ClusterService.class);
        indicesService = getInstanceFromNode(IndicesService.class);
        CapturingTransport capturingTransport = new CapturingTransport();
        transportService = capturingTransport.createTransportService(
            clusterService.getSettings(),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundAddress -> clusterService.localNode(),
            null,
            emptySet(),
            NoopTracer.INSTANCE
        );
        transportService.start();
        transportService.acceptIncomingRequests();
        context = new Context(randomAlphaOfLength(5));
        getIndexAction = new GetIndexActionTests.TestTransportGetIndexAction();
    }

    @After
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
        super.tearDown();
    }

    public void testIncludeDefaults() {
        GetIndexRequest defaultsRequest = new GetIndexRequest().indices(indexName).includeDefaults(true);
        getIndexAction.execute(
            null,
            defaultsRequest,
            ActionListener.wrap(
                defaultsResponse -> assertNotNull(
                    "index.refresh_interval should be set as we are including defaults",
                    defaultsResponse.getSetting(indexName, "index.refresh_interval")
                ),
                exception -> {
                    throw new AssertionError(exception);
                }
            )
        );
    }

    public void testDoNotIncludeDefaults() {
        GetIndexRequest noDefaultsRequest = new GetIndexRequest().indices(indexName);
        getIndexAction.execute(
            null,
            noDefaultsRequest,
            ActionListener.wrap(
                noDefaultsResponse -> assertNull(
                    "index.refresh_interval should be null as it was never set",
                    noDefaultsResponse.getSetting(indexName, "index.refresh_interval")
                ),
                exception -> {
                    throw new AssertionError(exception);
                }
            )
        );
    }

    public void testContextInResponse() {
        GetIndexRequest contextIndexRequest = new GetIndexRequest().indices(indexName);
        getIndexAction.execute(
            null,
            contextIndexRequest,
            ActionListener.wrap(
                resp -> assertTrue(
                    "index context should be present as it was set",
                    resp.contexts().get(indexName) != null && resp.contexts().get(indexName).equals(context)
                ),
                exception -> {
                    throw new AssertionError(exception);
                }
            )
        );
    }

    class TestTransportGetIndexAction extends TransportGetIndexAction {

        TestTransportGetIndexAction() {
            super(
                GetIndexActionTests.this.transportService,
                GetIndexActionTests.this.clusterService,
                GetIndexActionTests.this.threadPool,
                settingsFilter,
                new ActionFilters(emptySet()),
                new GetIndexActionTests.Resolver(),
                indicesService,
                IndexScopedSettings.DEFAULT_SCOPED_SETTINGS
            );
        }

        @Override
        protected void doClusterManagerOperation(
            GetIndexRequest request,
            String[] concreteIndices,
            ClusterState state,
            ActionListener<GetIndexResponse> listener
        ) {
            ClusterState stateWithIndex = ClusterStateCreationUtils.stateWithContext(indexName, 1, 1, context);
            super.doClusterManagerOperation(request, concreteIndices, stateWithIndex, listener);
        }
    }

    static class Resolver extends IndexNameExpressionResolver {
        Resolver() {
            super(new ThreadContext(Settings.EMPTY));
        }

        @Override
        public String[] concreteIndexNames(ClusterState state, IndicesRequest request) {
            return request.indices();
        }

        @Override
        public Index[] concreteIndices(ClusterState state, IndicesRequest request) {
            Index[] out = new Index[request.indices().length];
            for (int x = 0; x < out.length; x++) {
                out[x] = new Index(request.indices()[x], "_na_");
            }
            return out;
        }
    }
}
