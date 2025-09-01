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

package org.density.action.admin.indices.mapping.get;

import org.density.Version;
import org.density.action.support.ActionFilters;
import org.density.action.support.clustermanager.term.GetTermVersionResponse;
import org.density.action.support.replication.ClusterStateCreationUtils;
import org.density.cluster.ClusterState;
import org.density.cluster.block.ClusterBlock;
import org.density.cluster.block.ClusterBlockException;
import org.density.cluster.block.ClusterBlockLevel;
import org.density.cluster.block.ClusterBlocks;
import org.density.cluster.coordination.ClusterStateTermVersion;
import org.density.cluster.metadata.IndexNameExpressionResolver;
import org.density.cluster.node.DiscoveryNode;
import org.density.cluster.node.DiscoveryNodeRole;
import org.density.cluster.service.ClusterService;
import org.density.common.settings.Settings;
import org.density.common.settings.SettingsFilter;
import org.density.common.settings.SettingsModule;
import org.density.common.util.concurrent.ThreadContext;
import org.density.core.action.ActionListener;
import org.density.core.rest.RestStatus;
import org.density.indices.IndicesService;
import org.density.telemetry.tracing.noop.NoopTracer;
import org.density.test.DensityTestCase;
import org.density.test.transport.CapturingTransport;
import org.density.threadpool.TestThreadPool;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportService;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static org.density.test.ClusterServiceUtils.createClusterService;
import static org.density.test.ClusterServiceUtils.setState;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class GetMappingsActionTests extends DensityTestCase {
    private TransportService transportService;
    private ClusterService clusterService;
    private ThreadPool threadPool;
    private SettingsFilter settingsFilter;
    private final String indexName = "test_index";
    CapturingTransport capturingTransport = new CapturingTransport();
    private DiscoveryNode localNode;
    private DiscoveryNode remoteNode;
    private DiscoveryNode[] allNodes;
    private TransportGetMappingsAction transportAction = null;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        settingsFilter = new SettingsModule(Settings.EMPTY, emptyList(), emptyList(), emptySet()).getSettingsFilter();
        threadPool = new TestThreadPool("GetIndexActionTests");
        clusterService = createClusterService(threadPool);

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

        localNode = new DiscoveryNode(
            "local_node",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Collections.singleton(DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        );
        remoteNode = new DiscoveryNode(
            "remote_node",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Collections.singleton(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE),
            Version.CURRENT
        );
        allNodes = new DiscoveryNode[] { localNode, remoteNode };
        setState(clusterService, ClusterStateCreationUtils.state(localNode, remoteNode, allNodes));
        transportAction = new TransportGetMappingsAction(
            GetMappingsActionTests.this.transportService,
            GetMappingsActionTests.this.clusterService,
            GetMappingsActionTests.this.threadPool,
            new ActionFilters(emptySet()),
            new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY)),
            mock(IndicesService.class)
        );

    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        clusterService.close();
        transportService.close();
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
    }

    public void testGetTransportWithoutMatchingTerm() {
        transportAction.execute(null, new GetMappingsRequest(), ActionListener.wrap(Assert::assertNotNull, exception -> {
            throw new AssertionError(exception);
        }));
        assertThat(capturingTransport.capturedRequests().length, equalTo(1));
        CapturingTransport.CapturedRequest capturedRequest = capturingTransport.capturedRequests()[0];
        // mismatch term and version
        GetTermVersionResponse termResp = new GetTermVersionResponse(
            new ClusterStateTermVersion(
                clusterService.state().getClusterName(),
                clusterService.state().metadata().clusterUUID(),
                clusterService.state().term() - 1,
                clusterService.state().version() - 1
            )
        );
        capturingTransport.handleResponse(capturedRequest.requestId, termResp);

        assertThat(capturingTransport.capturedRequests().length, equalTo(2));
        CapturingTransport.CapturedRequest capturedRequest1 = capturingTransport.capturedRequests()[1];

        capturingTransport.handleResponse(capturedRequest1.requestId, new GetMappingsResponse(new HashMap<>()));
    }

    public void testGetTransportWithMatchingTerm() {
        transportAction.execute(null, new GetMappingsRequest(), ActionListener.wrap(Assert::assertNotNull, exception -> {
            throw new AssertionError(exception);
        }));
        assertThat(capturingTransport.capturedRequests().length, equalTo(1));
        CapturingTransport.CapturedRequest capturedRequest = capturingTransport.capturedRequests()[0];
        GetTermVersionResponse termResp = new GetTermVersionResponse(
            new ClusterStateTermVersion(
                clusterService.state().getClusterName(),
                clusterService.state().metadata().clusterUUID(),
                clusterService.state().term(),
                clusterService.state().version()
            )
        );
        capturingTransport.handleResponse(capturedRequest.requestId, termResp);

        // no more transport calls
        assertThat(capturingTransport.capturedRequests().length, equalTo(1));
    }

    public void testGetTransportClusterBlockWithMatchingTerm() {
        ClusterBlock readClusterBlock = new ClusterBlock(
            1,
            "uuid",
            "",
            false,
            true,
            true,
            RestStatus.OK,
            EnumSet.of(ClusterBlockLevel.METADATA_READ)
        );
        ClusterBlocks.Builder builder = ClusterBlocks.builder();
        builder.addGlobalBlock(readClusterBlock);
        ClusterState metadataReadBlockedState = ClusterState.builder(ClusterStateCreationUtils.state(localNode, remoteNode, allNodes))
            .blocks(builder)
            .build();
        setState(clusterService, metadataReadBlockedState);

        transportAction.execute(
            null,
            new GetMappingsRequest(),
            ActionListener.wrap(response -> { throw new AssertionError(response); }, exception -> {
                Assert.assertTrue(exception instanceof ClusterBlockException);
            })
        );
        assertThat(capturingTransport.capturedRequests().length, equalTo(1));
        CapturingTransport.CapturedRequest capturedRequest = capturingTransport.capturedRequests()[0];
        GetTermVersionResponse termResp = new GetTermVersionResponse(
            new ClusterStateTermVersion(
                clusterService.state().getClusterName(),
                clusterService.state().metadata().clusterUUID(),
                clusterService.state().term(),
                clusterService.state().version()
            )
        );
        capturingTransport.handleResponse(capturedRequest.requestId, termResp);

        // no more transport calls
        assertThat(capturingTransport.capturedRequests().length, equalTo(1));
    }
}
