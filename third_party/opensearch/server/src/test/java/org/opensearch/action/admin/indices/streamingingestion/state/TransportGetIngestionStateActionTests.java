/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.indices.streamingingestion.state;

import org.apache.lucene.store.AlreadyClosedException;
import org.density.Version;
import org.density.action.support.ActionFilters;
import org.density.cluster.ClusterState;
import org.density.cluster.block.ClusterBlockException;
import org.density.cluster.block.ClusterBlockLevel;
import org.density.cluster.metadata.IndexNameExpressionResolver;
import org.density.cluster.routing.ShardRouting;
import org.density.cluster.routing.ShardsIterator;
import org.density.cluster.service.ClusterService;
import org.density.common.settings.Settings;
import org.density.core.action.support.DefaultShardOperationFailedException;
import org.density.core.index.Index;
import org.density.core.index.shard.ShardId;
import org.density.index.IndexService;
import org.density.index.shard.IndexShard;
import org.density.index.shard.ShardNotFoundException;
import org.density.indices.IndicesService;
import org.density.telemetry.tracing.noop.NoopTracer;
import org.density.test.DensityTestCase;
import org.density.test.transport.MockTransportService;
import org.density.threadpool.TestThreadPool;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportService;
import org.density.transport.client.node.NodeClient;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportGetIngestionStateActionTests extends DensityTestCase {

    private TestThreadPool threadPool;
    private ClusterService clusterService;
    private TransportService transportService;
    private IndicesService indicesService;
    private ActionFilters actionFilters;
    private IndexNameExpressionResolver indexNameExpressionResolver;
    private TransportGetIngestionStateAction action;
    private NodeClient client;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getClass().getName());
        clusterService = mock(ClusterService.class);
        transportService = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool, NoopTracer.INSTANCE);
        indicesService = mock(IndicesService.class);
        actionFilters = mock(ActionFilters.class);
        indexNameExpressionResolver = mock(IndexNameExpressionResolver.class);
        client = mock(NodeClient.class);

        action = new TransportGetIngestionStateAction(
            clusterService,
            transportService,
            indicesService,
            actionFilters,
            indexNameExpressionResolver,
            client
        );
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadPool.terminate(threadPool, 30, java.util.concurrent.TimeUnit.SECONDS);
    }

    public void testShards() {
        GetIngestionStateRequest request = new GetIngestionStateRequest(new String[] { "test-index" });
        request.setShards(new int[] { 0, 1 });
        ClusterState clusterState = mock(ClusterState.class);
        ShardsIterator shardsIterator = mock(ShardsIterator.class);
        when(clusterState.routingTable()).thenReturn(mock(org.density.cluster.routing.RoutingTable.class));
        when(clusterState.routingTable().allShardsSatisfyingPredicate(any(), any())).thenReturn(shardsIterator);

        ShardsIterator result = action.shards(clusterState, request, new String[] { "test-index" });
        assertThat(result, equalTo(shardsIterator));
    }

    public void testCheckGlobalBlock() {
        GetIngestionStateRequest request = new GetIngestionStateRequest(new String[] { "test-index" });
        ClusterState clusterState = mock(ClusterState.class);
        when(clusterState.blocks()).thenReturn(mock(org.density.cluster.block.ClusterBlocks.class));
        when(clusterState.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ)).thenReturn(null);

        ClusterBlockException result = action.checkGlobalBlock(clusterState, request);
        assertThat(result, equalTo(null));
    }

    public void testCheckRequestBlock() {
        GetIngestionStateRequest request = new GetIngestionStateRequest(new String[] { "test-index" });
        ClusterState clusterState = mock(ClusterState.class);
        when(clusterState.blocks()).thenReturn(mock(org.density.cluster.block.ClusterBlocks.class));
        when(clusterState.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ, request.indices())).thenReturn(null);

        ClusterBlockException result = action.checkRequestBlock(clusterState, request, new String[] { "test-index" });
        assertThat(result, equalTo(null));
    }

    public void testShardOperation() {
        GetIngestionStateRequest request = new GetIngestionStateRequest(new String[] { "test-index" });
        ShardRouting shardRouting = mock(ShardRouting.class);
        IndexService indexService = mock(IndexService.class);
        IndexShard indexShard = mock(IndexShard.class);
        ShardIngestionState expectedState = new ShardIngestionState("test-index", 0, "POLLING", "DROP", true, false, "");

        when(shardRouting.shardId()).thenReturn(mock(ShardId.class));
        when(shardRouting.shardId().getIndex()).thenReturn(mock(Index.class));
        when(shardRouting.shardId().id()).thenReturn(0);
        when(indicesService.indexServiceSafe(any())).thenReturn(indexService);
        when(indexService.getShard(0)).thenReturn(indexShard);
        when(indexShard.routingEntry()).thenReturn(mock(org.density.cluster.routing.ShardRouting.class));
        when(indexShard.getIngestionState()).thenReturn(expectedState);

        ShardIngestionState result = action.shardOperation(request, shardRouting);
        assertThat(result, equalTo(expectedState));
    }

    public void testShardOperationWithShardNotFoundException() {
        GetIngestionStateRequest request = new GetIngestionStateRequest(new String[] { "test-index" });
        ShardRouting shardRouting = mock(ShardRouting.class);
        IndexService indexService = mock(IndexService.class);
        IndexShard indexShard = mock(IndexShard.class);

        when(shardRouting.shardId()).thenReturn(mock(ShardId.class));
        when(shardRouting.shardId().getIndex()).thenReturn(mock(Index.class));
        when(shardRouting.shardId().id()).thenReturn(0);
        when(indicesService.indexServiceSafe(any())).thenReturn(indexService);
        when(indexService.getShard(0)).thenReturn(indexShard);
        when(indexShard.routingEntry()).thenReturn(null);

        expectThrows(ShardNotFoundException.class, () -> action.shardOperation(request, shardRouting));
    }

    public void testShardOperationWithAlreadyClosedException() {
        GetIngestionStateRequest request = new GetIngestionStateRequest(new String[] { "test-index" });
        ShardRouting shardRouting = mock(ShardRouting.class);
        IndexService indexService = mock(IndexService.class);
        IndexShard indexShard = mock(IndexShard.class);

        when(shardRouting.shardId()).thenReturn(mock(ShardId.class));
        when(shardRouting.shardId().getIndex()).thenReturn(mock(Index.class));
        when(shardRouting.shardId().id()).thenReturn(0);
        when(indicesService.indexServiceSafe(any())).thenReturn(indexService);
        when(indexService.getShard(0)).thenReturn(indexShard);
        when(indexShard.routingEntry()).thenReturn(mock(org.density.cluster.routing.ShardRouting.class));
        when(indexShard.getIngestionState()).thenThrow(new AlreadyClosedException("shard is closed"));

        expectThrows(ShardNotFoundException.class, () -> action.shardOperation(request, shardRouting));
    }

    public void testNewResponse() {
        GetIngestionStateRequest request = new GetIngestionStateRequest(new String[] { "test-index" });
        List<ShardIngestionState> responses = Collections.singletonList(
            new ShardIngestionState("test-index", 0, "POLLING", "DROP", true, false, "")
        );
        List<DefaultShardOperationFailedException> shardFailures = Collections.emptyList();
        ClusterState clusterState = mock(ClusterState.class);

        GetIngestionStateResponse response = action.newResponse(request, 1, 1, 0, responses, shardFailures, clusterState);

        assertThat(response.getTotalShards(), equalTo(1));
        assertThat(response.getSuccessfulShards(), equalTo(1));
        assertThat(response.getFailedShards(), equalTo(0));
        assertThat(response.getShardStates().length, equalTo(1));
        assertThat(response.getShardStates()[0].index(), equalTo("test-index"));
        assertThat(response.getShardStates()[0].shardId(), equalTo(0));
    }
}
