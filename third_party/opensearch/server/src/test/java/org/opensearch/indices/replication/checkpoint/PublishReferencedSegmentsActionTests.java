/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.indices.replication.checkpoint;

import org.apache.lucene.codecs.Codec;
import org.density.action.LatchedActionListener;
import org.density.action.support.ActionFilters;
import org.density.action.support.ActionTestUtils;
import org.density.action.support.PlainActionFuture;
import org.density.action.support.replication.TransportReplicationAction;
import org.density.cluster.action.shard.ShardStateAction;
import org.density.cluster.metadata.IndexMetadata;
import org.density.cluster.routing.AllocationId;
import org.density.cluster.routing.ShardRouting;
import org.density.cluster.service.ClusterService;
import org.density.common.settings.Settings;
import org.density.common.unit.TimeValue;
import org.density.common.util.io.IOUtils;
import org.density.common.util.set.Sets;
import org.density.core.action.ActionListener;
import org.density.core.index.Index;
import org.density.core.index.shard.ShardId;
import org.density.index.IndexService;
import org.density.index.shard.IndexShard;
import org.density.indices.IndicesService;
import org.density.indices.recovery.RecoverySettings;
import org.density.indices.recovery.RecoveryState;
import org.density.telemetry.tracing.noop.NoopTracer;
import org.density.test.DensityTestCase;
import org.density.test.transport.CapturingTransport;
import org.density.threadpool.TestThreadPool;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportService;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.density.index.remote.RemoteStoreTestsHelper.createIndexSettings;
import static org.density.test.ClusterServiceUtils.createClusterService;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PublishReferencedSegmentsActionTests extends DensityTestCase {

    private ThreadPool threadPool;
    private CapturingTransport transport;
    private ClusterService clusterService;
    private TransportService transportService;
    private ShardStateAction shardStateAction;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getClass().getName());
        transport = new CapturingTransport();
        clusterService = createClusterService(threadPool);
        transportService = transport.createTransportService(
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
        shardStateAction = new ShardStateAction(clusterService, transportService, null, null, threadPool);
    }

    @Override
    public void tearDown() throws Exception {
        try {
            IOUtils.close(transportService, clusterService, transport);
        } finally {
            terminate(threadPool);
        }
        super.tearDown();
    }

    public void testPublishReferencedSegments() {
        final IndicesService indicesService = mock(IndicesService.class);

        final Index index = new Index("index", "uuid");
        final IndexService indexService = mock(IndexService.class);
        when(indicesService.indexServiceSafe(index)).thenReturn(indexService);

        final int id = randomIntBetween(0, 4);
        final IndexShard indexShard = mock(IndexShard.class);
        when(indexService.getShard(id)).thenReturn(indexShard);

        final ShardId shardId = new ShardId(index, id);
        when(indexShard.shardId()).thenReturn(shardId);

        ShardRouting shardRouting = mock(ShardRouting.class);
        AllocationId allocationId = mock(AllocationId.class);
        RecoveryState recoveryState = mock(RecoveryState.class);
        RecoverySettings recoverySettings = mock(RecoverySettings.class);
        when(recoverySettings.getMergedSegmentReplicationTimeout()).thenReturn(new TimeValue(1000));
        when(shardRouting.allocationId()).thenReturn(allocationId);
        when(allocationId.getId()).thenReturn("1");
        when(recoveryState.getTargetNode()).thenReturn(clusterService.localNode());
        when(indexShard.routingEntry()).thenReturn(shardRouting);
        when(indexShard.getPendingPrimaryTerm()).thenReturn(1L);
        when(indexShard.recoveryState()).thenReturn(recoveryState);
        when(indexShard.getRecoverySettings()).thenReturn(recoverySettings);

        final PublishReferencedSegmentsAction action = new PublishReferencedSegmentsAction(
            Settings.EMPTY,
            transportService,
            clusterService,
            indicesService,
            threadPool,
            shardStateAction,
            new ActionFilters(Collections.emptySet())
        );

        final ReferencedSegmentsCheckpoint checkpoint = new ReferencedSegmentsCheckpoint(
            indexShard.shardId(),
            1,
            1,
            1111,
            Codec.getDefault().getName(),
            Collections.emptyMap(),
            Sets.newHashSet("_1", "_2", "_3")
        );

        action.publish(indexShard, checkpoint);
    }

    public void testPublishReferencedSegmentsOnPrimary() throws Exception {
        final IndicesService indicesService = mock(IndicesService.class);

        final Index index = new Index("index", "uuid");
        final IndexService indexService = mock(IndexService.class);
        when(indicesService.indexServiceSafe(index)).thenReturn(indexService);

        final int id = randomIntBetween(0, 4);
        final IndexShard indexShard = mock(IndexShard.class);
        when(indexService.getShard(id)).thenReturn(indexShard);

        final ShardId shardId = new ShardId(index, id);
        when(indexShard.shardId()).thenReturn(shardId);

        final PublishReferencedSegmentsAction action = new PublishReferencedSegmentsAction(
            Settings.EMPTY,
            transportService,
            clusterService,
            indicesService,
            threadPool,
            shardStateAction,
            new ActionFilters(Collections.emptySet())
        );

        final ReferencedSegmentsCheckpoint checkpoint = new ReferencedSegmentsCheckpoint(
            indexShard.shardId(),
            1,
            1,
            1111,
            Codec.getDefault().getName(),
            Collections.emptyMap(),
            Sets.newHashSet("_1", "_2", "_3")
        );
        final PublishReferencedSegmentsRequest request = new PublishReferencedSegmentsRequest(checkpoint);
        final CountDownLatch latch = new CountDownLatch(1);
        action.shardOperationOnPrimary(request, indexShard, new LatchedActionListener<>(ActionTestUtils.assertNoFailureListener(result -> {
            // we should forward the request containing the current publish checkpoint to the replica
            assertThat(result.replicaRequest(), sameInstance(request));
        }), latch));
        latch.await();
    }

    public void testPublishReferencedSegmentsActionOnReplica() {
        final IndicesService indicesService = mock(IndicesService.class);

        final Index index = new Index("index", "uuid");
        final IndexService indexService = mock(IndexService.class);
        when(indicesService.indexServiceSafe(index)).thenReturn(indexService);
        final int id = randomIntBetween(0, 4);
        final IndexShard indexShard = mock(IndexShard.class);
        when(indexService.getShard(id)).thenReturn(indexShard);

        final ShardId shardId = new ShardId(index, id);
        when(indexShard.shardId()).thenReturn(shardId);
        when(indexShard.indexSettings()).thenReturn(
            createIndexSettings(false, Settings.builder().put(IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.getKey(), "SEGMENT").build())
        );

        final PublishReferencedSegmentsAction action = new PublishReferencedSegmentsAction(
            Settings.EMPTY,
            transportService,
            clusterService,
            indicesService,
            threadPool,
            shardStateAction,
            new ActionFilters(Collections.emptySet())
        );

        final ReferencedSegmentsCheckpoint checkpoint = new ReferencedSegmentsCheckpoint(
            indexShard.shardId(),
            1,
            1,
            1111,
            Codec.getDefault().getName(),
            Collections.emptyMap(),
            Sets.newHashSet("_1", "_2", "_3")
        );

        final PublishReferencedSegmentsRequest request = new PublishReferencedSegmentsRequest(checkpoint);

        final PlainActionFuture<TransportReplicationAction.ReplicaResult> listener = PlainActionFuture.newFuture();
        action.shardOperationOnReplica(request, indexShard, listener);
        final TransportReplicationAction.ReplicaResult result = listener.actionGet();

        // cleanupRedundantPendingMergeSegment should be called on replica shard
        verify(indexShard, times(1)).cleanupRedundantPendingMergeSegment(checkpoint);

        // the result should indicate success
        final AtomicBoolean success = new AtomicBoolean();
        result.runPostReplicaActions(ActionListener.wrap(r -> success.set(true), e -> fail(e.toString())));
        assertTrue(success.get());
    }
}
