/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.indices.replication;

import org.apache.lucene.codecs.Codec;
import org.density.Version;
import org.density.cluster.ClusterChangedEvent;
import org.density.cluster.node.DiscoveryNode;
import org.density.cluster.routing.ShardRouting;
import org.density.common.io.stream.BytesStreamOutput;
import org.density.common.settings.ClusterSettings;
import org.density.common.settings.Settings;
import org.density.core.action.ActionListener;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.index.shard.ShardId;
import org.density.core.transport.TransportResponse;
import org.density.index.IndexService;
import org.density.index.IndexSettings;
import org.density.index.shard.IndexShard;
import org.density.index.shard.IndexShardState;
import org.density.index.shard.ReplicationGroup;
import org.density.indices.IndicesService;
import org.density.indices.recovery.RecoverySettings;
import org.density.indices.replication.checkpoint.ReplicationCheckpoint;
import org.density.indices.replication.common.CopyStateTests;
import org.density.indices.replication.common.ReplicationType;
import org.density.telemetry.tracing.noop.NoopTracer;
import org.density.test.IndexSettingsModule;
import org.density.test.DensityTestCase;
import org.density.test.transport.CapturingTransport;
import org.density.threadpool.TestThreadPool;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportException;
import org.density.transport.TransportResponseHandler;
import org.density.transport.TransportService;
import org.junit.Assert;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.density.cluster.metadata.IndexMetadata.INDEX_REPLICATION_TYPE_SETTING;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SegmentReplicationSourceServiceTests extends DensityTestCase {

    private ReplicationCheckpoint testCheckpoint;
    private TestThreadPool testThreadPool;
    private TransportService transportService;
    private DiscoveryNode localNode;
    private SegmentReplicationSourceService segmentReplicationSourceService;
    private OngoingSegmentReplications ongoingSegmentReplications;
    private IndexShard mockIndexShard;
    private IndicesService mockIndicesService;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // setup mocks
        mockIndexShard = CopyStateTests.createMockIndexShard();
        ShardId testShardId = mockIndexShard.shardId();
        mockIndicesService = mock(IndicesService.class);
        IndexService mockIndexService = mock(IndexService.class);
        when(mockIndicesService.iterator()).thenReturn(List.of(mockIndexService).iterator());
        when(mockIndicesService.indexServiceSafe(testShardId.getIndex())).thenReturn(mockIndexService);
        when(mockIndexService.getShard(testShardId.id())).thenReturn(mockIndexShard);
        when(mockIndexService.iterator()).thenReturn(List.of(mockIndexShard).iterator());
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(
            "index",
            Settings.builder().put(INDEX_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.SEGMENT).build()
        );
        when(mockIndexService.getIndexSettings()).thenReturn(indexSettings);
        final ShardRouting routing = mock(ShardRouting.class);
        when(routing.primary()).thenReturn(true);
        when(mockIndexShard.routingEntry()).thenReturn(routing);
        when(mockIndexShard.isPrimaryMode()).thenReturn(true);
        final ReplicationGroup replicationGroup = mock(ReplicationGroup.class);
        when(mockIndexShard.getReplicationGroup()).thenReturn(replicationGroup);
        when(mockIndexShard.state()).thenReturn(IndexShardState.STARTED);
        when(replicationGroup.getInSyncAllocationIds()).thenReturn(Collections.emptySet());
        // This mirrors the creation of the ReplicationCheckpoint inside CopyState
        testCheckpoint = new ReplicationCheckpoint(
            testShardId,
            mockIndexShard.getOperationPrimaryTerm(),
            0L,
            0L,
            Codec.getDefault().getName()
        );
        testThreadPool = new TestThreadPool("test", Settings.EMPTY);
        CapturingTransport transport = new CapturingTransport();
        localNode = new DiscoveryNode("local", buildNewFakeTransportAddress(), Version.CURRENT);
        transportService = transport.createTransportService(
            Settings.EMPTY,
            testThreadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundAddress -> localNode,
            null,
            Collections.emptySet(),
            NoopTracer.INSTANCE
        );
        transportService.start();
        transportService.acceptIncomingRequests();

        final Settings settings = Settings.builder().put("node.name", SegmentReplicationTargetServiceTests.class.getSimpleName()).build();
        final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final RecoverySettings recoverySettings = new RecoverySettings(settings, clusterSettings);
        when(mockIndexShard.getRecoverySettings()).thenReturn(recoverySettings);
        when(mockIndexShard.getThreadPool()).thenReturn(testThreadPool);

        ongoingSegmentReplications = spy(new OngoingSegmentReplications(mockIndicesService, recoverySettings));
        segmentReplicationSourceService = new SegmentReplicationSourceService(
            mockIndicesService,
            transportService,
            recoverySettings,
            ongoingSegmentReplications
        );
    }

    @Override
    public void tearDown() throws Exception {
        ThreadPool.terminate(testThreadPool, 30, TimeUnit.SECONDS);
        testThreadPool = null;
        super.tearDown();
    }

    public void testGetSegmentFiles() {
        final GetSegmentFilesRequest request = new GetSegmentFilesRequest(
            1,
            "allocationId",
            localNode,
            Collections.emptyList(),
            testCheckpoint
        );
        executeGetSegmentFiles(request, new ActionListener<>() {
            @Override
            public void onResponse(GetSegmentFilesResponse response) {
                assertEquals(0, response.files.size());
            }

            @Override
            public void onFailure(Exception e) {
                fail("unexpected exception: " + e);
            }
        });
    }

    public void testGetMergedSegmentFiles() {
        final GetSegmentFilesRequest request = new GetSegmentFilesRequest(
            1,
            "allocationId",
            localNode,
            Collections.emptyList(),
            testCheckpoint
        );
        executeGetMergedSegmentFiles(request, new ActionListener<>() {
            @Override
            public void onResponse(GetSegmentFilesResponse response) {
                assertEquals(0, response.files.size());
            }

            @Override
            public void onFailure(Exception e) {
                fail("unexpected exception: " + e);
            }
        });
    }

    public void testGetMergedSegmentFiles_shardClosed() {
        when(mockIndexShard.state()).thenReturn(IndexShardState.CLOSED);
        final GetSegmentFilesRequest request = new GetSegmentFilesRequest(
            1,
            "allocationId",
            localNode,
            Collections.emptyList(),
            testCheckpoint
        );
        executeGetMergedSegmentFiles(request, new ActionListener<>() {
            @Override
            public void onResponse(GetSegmentFilesResponse response) {
                Assert.fail("Test should fail");
            }

            @Override
            public void onFailure(Exception e) {
                assert e.getCause() instanceof IllegalStateException;
            }
        });
    }

    public void testGetMergedSegmentFiles_shardNonPrimary() {
        when(mockIndexShard.isPrimaryMode()).thenReturn(false);
        final GetSegmentFilesRequest request = new GetSegmentFilesRequest(
            1,
            "allocationId",
            localNode,
            Collections.emptyList(),
            testCheckpoint
        );
        executeGetMergedSegmentFiles(request, new ActionListener<>() {
            @Override
            public void onResponse(GetSegmentFilesResponse response) {
                Assert.fail("Test should fail");
            }

            @Override
            public void onFailure(Exception e) {
                assert e.getCause() instanceof IllegalStateException;
            }
        });
    }

    public void testUpdateVisibleCheckpoint() {
        UpdateVisibleCheckpointRequest request = new UpdateVisibleCheckpointRequest(
            0L,
            "",
            mockIndexShard.shardId(),
            localNode,
            testCheckpoint
        );
        executeUpdateVisibleCheckpoint(request, new ActionListener<>() {
            @Override
            public void onResponse(TransportResponse transportResponse) {
                assertTrue(TransportResponse.Empty.INSTANCE.equals(transportResponse));
            }

            @Override
            public void onFailure(Exception e) {
                fail("unexpected exception: " + e);
            }
        });
    }

    public void testCheckpointInfo() {
        executeGetCheckpointInfo(new ActionListener<>() {
            @Override
            public void onResponse(CheckpointInfoResponse response) {
                assertEquals(testCheckpoint, response.getCheckpoint());
                assertNotNull(response.getInfosBytes());
                assertEquals(1, response.getMetadataMap().size());
            }

            @Override
            public void onFailure(Exception e) {
                fail("unexpected exception: " + e);
            }
        });
    }

    public void testPrimaryClearsOutOfSyncIds() {
        final ClusterChangedEvent mock = mock(ClusterChangedEvent.class);
        when(mock.routingTableChanged()).thenReturn(true);
        segmentReplicationSourceService.clusterChanged(mock);
        verify(ongoingSegmentReplications, times(1)).clearOutOfSyncIds(any(), any());
    }

    private void executeGetCheckpointInfo(ActionListener<CheckpointInfoResponse> listener) {
        final CheckpointInfoRequest request = new CheckpointInfoRequest(1L, "testAllocationId", localNode, testCheckpoint);
        transportService.sendRequest(
            localNode,
            SegmentReplicationSourceService.Actions.GET_CHECKPOINT_INFO,
            request,
            new TransportResponseHandler<CheckpointInfoResponse>() {
                @Override
                public void handleResponse(CheckpointInfoResponse response) {
                    listener.onResponse(response);
                }

                @Override
                public void handleException(TransportException e) {
                    listener.onFailure(e);
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }

                @Override
                public CheckpointInfoResponse read(StreamInput in) throws IOException {
                    return new CheckpointInfoResponse(in);
                }
            }
        );
    }

    private void executeGetSegmentFiles(GetSegmentFilesRequest request, ActionListener<GetSegmentFilesResponse> listener) {
        transportService.sendRequest(
            localNode,
            SegmentReplicationSourceService.Actions.GET_SEGMENT_FILES,
            request,
            new TransportResponseHandler<GetSegmentFilesResponse>() {
                @Override
                public void handleResponse(GetSegmentFilesResponse response) {
                    listener.onResponse(response);
                }

                @Override
                public void handleException(TransportException e) {
                    listener.onFailure(e);
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }

                @Override
                public GetSegmentFilesResponse read(StreamInput in) throws IOException {
                    return new GetSegmentFilesResponse(in);
                }
            }
        );
    }

    private void executeGetMergedSegmentFiles(GetSegmentFilesRequest request, ActionListener<GetSegmentFilesResponse> listener) {
        transportService.sendRequest(
            localNode,
            SegmentReplicationSourceService.Actions.GET_MERGED_SEGMENT_FILES,
            request,
            new TransportResponseHandler<GetSegmentFilesResponse>() {
                @Override
                public void handleResponse(GetSegmentFilesResponse response) {
                    listener.onResponse(response);
                }

                @Override
                public void handleException(TransportException e) {
                    listener.onFailure(e);
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }

                @Override
                public GetSegmentFilesResponse read(StreamInput in) throws IOException {
                    return new GetSegmentFilesResponse(in);
                }
            }
        );
    }

    private void executeUpdateVisibleCheckpoint(UpdateVisibleCheckpointRequest request, ActionListener<TransportResponse> listener) {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                UpdateVisibleCheckpointRequest newRequest = new UpdateVisibleCheckpointRequest(in);
                assertTrue(newRequest.getCheckpoint().equals(request.getCheckpoint()));
                assertTrue(newRequest.getTargetAllocationId().equals(request.getTargetAllocationId()));
            }
        } catch (IOException e) {
            fail("Failed to parse UpdateVisibleCheckpointRequest " + e);
        }

        transportService.sendRequest(
            localNode,
            SegmentReplicationSourceService.Actions.UPDATE_VISIBLE_CHECKPOINT,
            request,
            new TransportResponseHandler<>() {
                @Override
                public void handleResponse(TransportResponse response) {
                    listener.onResponse(TransportResponse.Empty.INSTANCE);
                }

                @Override
                public void handleException(TransportException e) {
                    listener.onFailure(e);
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }

                @Override
                public CheckpointInfoResponse read(StreamInput in) throws IOException {
                    return new CheckpointInfoResponse(in);
                }
            }
        );
    }
}
