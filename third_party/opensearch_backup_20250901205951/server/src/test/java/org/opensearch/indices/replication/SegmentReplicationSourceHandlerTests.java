/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.indices.replication;

import org.density.DensityException;
import org.density.Version;
import org.density.cluster.metadata.IndexMetadata;
import org.density.cluster.node.DiscoveryNode;
import org.density.common.settings.Settings;
import org.density.common.util.CancellableThreads;
import org.density.core.action.ActionListener;
import org.density.core.xcontent.MediaTypeRegistry;
import org.density.index.engine.NRTReplicationEngineFactory;
import org.density.index.shard.IndexShard;
import org.density.index.shard.IndexShardTestCase;
import org.density.index.store.StoreFileMetadata;
import org.density.indices.recovery.FileChunkWriter;
import org.density.indices.replication.checkpoint.ReplicationCheckpoint;
import org.density.indices.replication.common.CopyState;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Assert;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.mockito.Mockito;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class SegmentReplicationSourceHandlerTests extends IndexShardTestCase {

    private final DiscoveryNode localNode = new DiscoveryNode("local", buildNewFakeTransportAddress(), Version.CURRENT);
    private DiscoveryNode replicaDiscoveryNode;
    private IndexShard primary;
    private IndexShard replica;

    private FileChunkWriter chunkWriter;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        final Settings settings = Settings.builder().put(IndexMetadata.SETTING_REPLICATION_TYPE, "SEGMENT").put(Settings.EMPTY).build();
        primary = newStartedShard(true, settings);
        replica = newShard(false, settings, new NRTReplicationEngineFactory());
        recoverReplica(replica, primary, true);
        replicaDiscoveryNode = replica.recoveryState().getTargetNode();
    }

    @Override
    public void tearDown() throws Exception {
        closeShards(primary, replica);
        super.tearDown();
    }

    public void testSendFiles() throws IOException {
        chunkWriter = (fileMetadata, position, content, lastChunk, totalTranslogOps, listener) -> listener.onResponse(null);

        final ReplicationCheckpoint latestReplicationCheckpoint = primary.getLatestReplicationCheckpoint();
        SegmentReplicationSourceHandler handler = new SegmentReplicationSourceHandler(
            localNode,
            chunkWriter,
            primary,
            replica.routingEntry().allocationId().getId(),
            5000,
            1
        );

        final List<StoreFileMetadata> expectedFiles = List.copyOf(handler.getCheckpoint().getMetadataMap().values());

        final GetSegmentFilesRequest getSegmentFilesRequest = new GetSegmentFilesRequest(
            1L,
            replica.routingEntry().allocationId().getId(),
            replicaDiscoveryNode,
            expectedFiles,
            latestReplicationCheckpoint
        );

        handler.sendFiles(getSegmentFilesRequest, new ActionListener<>() {
            @Override
            public void onResponse(GetSegmentFilesResponse getSegmentFilesResponse) {
                MatcherAssert.assertThat(getSegmentFilesResponse.files, Matchers.containsInAnyOrder(expectedFiles.toArray()));
            }

            @Override
            public void onFailure(Exception e) {
                Assert.fail();
            }
        });
    }

    public void testSendFiles_emptyRequest() throws IOException {
        chunkWriter = mock(FileChunkWriter.class);

        final ReplicationCheckpoint latestReplicationCheckpoint = primary.getLatestReplicationCheckpoint();
        SegmentReplicationSourceHandler handler = new SegmentReplicationSourceHandler(
            localNode,
            chunkWriter,
            primary,
            replica.routingEntry().allocationId().getId(),
            5000,
            1
        );

        final GetSegmentFilesRequest getSegmentFilesRequest = new GetSegmentFilesRequest(
            1L,
            replica.routingEntry().allocationId().getId(),
            replicaDiscoveryNode,
            Collections.emptyList(),
            latestReplicationCheckpoint
        );

        handler.sendFiles(getSegmentFilesRequest, new ActionListener<>() {
            @Override
            public void onResponse(GetSegmentFilesResponse getSegmentFilesResponse) {
                assertTrue(getSegmentFilesResponse.files.isEmpty());
                Mockito.verifyNoInteractions(chunkWriter);
            }

            @Override
            public void onFailure(Exception e) {
                Assert.fail();
            }
        });
    }

    public void testSendFileFails() throws IOException {
        // index some docs on the primary so a segment is created.
        indexDoc(primary, "1", "{\"foo\" : \"baz\"}", MediaTypeRegistry.JSON, "foobar");
        primary.refresh("Test");
        chunkWriter = (fileMetadata, position, content, lastChunk, totalTranslogOps, listener) -> listener.onFailure(
            new DensityException("Test")
        );

        final ReplicationCheckpoint latestReplicationCheckpoint = primary.getLatestReplicationCheckpoint();
        final CopyState copyState = new CopyState(primary);
        SegmentReplicationSourceHandler handler = new SegmentReplicationSourceHandler(
            localNode,
            chunkWriter,
            primary,
            primary.routingEntry().allocationId().getId(),
            5000,
            1
        );

        final List<StoreFileMetadata> expectedFiles = List.copyOf(copyState.getMetadataMap().values());

        final GetSegmentFilesRequest getSegmentFilesRequest = new GetSegmentFilesRequest(
            1L,
            replica.routingEntry().allocationId().getId(),
            replicaDiscoveryNode,
            expectedFiles,
            latestReplicationCheckpoint
        );

        handler.sendFiles(getSegmentFilesRequest, new ActionListener<>() {
            @Override
            public void onResponse(GetSegmentFilesResponse getSegmentFilesResponse) {
                Assert.fail();
            }

            @Override
            public void onFailure(Exception e) {
                assertEquals(e.getClass(), DensityException.class);
            }
        });
        copyState.close();
    }

    public void testReplicationAlreadyRunning() throws IOException {
        chunkWriter = mock(FileChunkWriter.class);

        final ReplicationCheckpoint latestReplicationCheckpoint = primary.getLatestReplicationCheckpoint();
        final CopyState copyState = new CopyState(primary);
        SegmentReplicationSourceHandler handler = new SegmentReplicationSourceHandler(
            localNode,
            chunkWriter,
            primary,
            replica.routingEntry().allocationId().getId(),
            5000,
            1
        );

        final List<StoreFileMetadata> expectedFiles = List.of(new StoreFileMetadata("_0.si", 20, "test", Version.CURRENT.luceneVersion));

        final GetSegmentFilesRequest getSegmentFilesRequest = new GetSegmentFilesRequest(
            1L,
            replica.routingEntry().allocationId().getId(),
            replicaDiscoveryNode,
            expectedFiles,
            latestReplicationCheckpoint
        );

        handler.sendFiles(getSegmentFilesRequest, mock(ActionListener.class));
        Assert.assertThrows(DensityException.class, () -> { handler.sendFiles(getSegmentFilesRequest, mock(ActionListener.class)); });
    }

    public void testCancelReplication() throws IOException, InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        chunkWriter = mock(FileChunkWriter.class);

        final ReplicationCheckpoint latestReplicationCheckpoint = primary.getLatestReplicationCheckpoint();
        SegmentReplicationSourceHandler handler = new SegmentReplicationSourceHandler(
            localNode,
            chunkWriter,
            primary,
            primary.routingEntry().allocationId().getId(),
            5000,
            1
        );

        final List<StoreFileMetadata> expectedFiles = List.of(new StoreFileMetadata("_0.si", 20, "test", Version.CURRENT.luceneVersion));
        final GetSegmentFilesRequest getSegmentFilesRequest = new GetSegmentFilesRequest(
            1L,
            replica.routingEntry().allocationId().getId(),
            replicaDiscoveryNode,
            expectedFiles,
            latestReplicationCheckpoint
        );

        // cancel before xfer starts. Cancels during copy will be tested in SegmentFileTransferHandlerTests, that uses the same
        // cancellableThreads.
        handler.cancel("test");
        handler.sendFiles(getSegmentFilesRequest, new ActionListener<>() {
            @Override
            public void onResponse(GetSegmentFilesResponse getSegmentFilesResponse) {
                Assert.fail("Expected failure.");
            }

            @Override
            public void onFailure(Exception e) {
                assertEquals(CancellableThreads.ExecutionCancelledException.class, e.getClass());
                latch.countDown();
            }
        });
        latch.await(2, TimeUnit.SECONDS);
        verify(chunkWriter, times(1)).cancel();
        assertEquals("listener should have resolved with failure", 0, latch.getCount());
    }
}
