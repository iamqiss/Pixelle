/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.gateway.remote.routingtable;

import org.density.cluster.ClusterState;
import org.density.cluster.Diff;
import org.density.cluster.routing.RoutingTable;
import org.density.cluster.routing.RoutingTableIncrementalDiff;
import org.density.common.blobstore.BlobPath;
import org.density.common.compress.DeflateCompressor;
import org.density.common.remote.BlobPathParameters;
import org.density.common.settings.ClusterSettings;
import org.density.common.settings.Settings;
import org.density.core.common.io.stream.NamedWriteableRegistry;
import org.density.core.compress.Compressor;
import org.density.core.compress.NoneCompressor;
import org.density.gateway.remote.ClusterMetadataManifest;
import org.density.index.remote.RemoteStoreUtils;
import org.density.index.translog.transfer.BlobStoreTransferService;
import org.density.repositories.blobstore.BlobStoreRepository;
import org.density.test.DensityTestCase;
import org.density.threadpool.TestThreadPool;
import org.density.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.density.gateway.remote.RemoteClusterStateServiceTests.generateClusterStateWithOneIndex;
import static org.density.gateway.remote.routingtable.RemoteRoutingTableDiff.ROUTING_TABLE_DIFF_FILE;
import static org.density.gateway.remote.routingtable.RemoteRoutingTableDiff.ROUTING_TABLE_DIFF_METADATA_PREFIX;
import static org.density.gateway.remote.routingtable.RemoteRoutingTableDiff.ROUTING_TABLE_DIFF_PATH_TOKEN;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RemoteIndexRoutingTableDiffTests extends DensityTestCase {

    private static final String TEST_BLOB_NAME = "/test-path/test-blob-name";
    private static final String TEST_BLOB_PATH = "test-path";
    private static final String TEST_BLOB_FILE_NAME = "test-blob-name";
    private static final long STATE_VERSION = 3L;
    private static final long STATE_TERM = 2L;
    private String clusterUUID;
    private BlobStoreRepository blobStoreRepository;
    private BlobStoreTransferService blobStoreTransferService;
    private ClusterSettings clusterSettings;
    private Compressor compressor;

    private String clusterName;
    private NamedWriteableRegistry namedWriteableRegistry;
    private final ThreadPool threadPool = new TestThreadPool(getClass().getName());

    @Before
    public void setup() {
        clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        this.clusterUUID = "test-cluster-uuid";
        this.blobStoreTransferService = mock(BlobStoreTransferService.class);
        this.blobStoreRepository = mock(BlobStoreRepository.class);
        BlobPath blobPath = new BlobPath().add("/path");
        when(blobStoreRepository.basePath()).thenReturn(blobPath);
        when(blobStoreRepository.getCompressor()).thenReturn(new DeflateCompressor());
        compressor = new NoneCompressor();
        namedWriteableRegistry = writableRegistry();
        this.clusterName = "test-cluster-name";
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdown();
    }

    public void testClusterUUID() {
        String indexName = randomAlphaOfLength(randomIntBetween(1, 50));
        ClusterState previousState = generateClusterStateWithOneIndex(indexName, 5, 1, false).build();
        ClusterState currentState = generateClusterStateWithOneIndex(indexName, 5, 2, true).build();

        RoutingTableIncrementalDiff routingTableIncrementalDiff = new RoutingTableIncrementalDiff(
            previousState.getRoutingTable(),
            currentState.getRoutingTable()
        );

        RemoteRoutingTableDiff remoteDiffForUpload = new RemoteRoutingTableDiff(
            routingTableIncrementalDiff,
            clusterUUID,
            compressor,
            STATE_TERM,
            STATE_VERSION
        );
        assertEquals(remoteDiffForUpload.clusterUUID(), clusterUUID);

        RemoteRoutingTableDiff remoteDiffForDownload = new RemoteRoutingTableDiff(TEST_BLOB_NAME, clusterUUID, compressor);
        assertEquals(remoteDiffForDownload.clusterUUID(), clusterUUID);
    }

    public void testFullBlobName() {
        String indexName = randomAlphaOfLength(randomIntBetween(1, 50));
        ClusterState previousState = generateClusterStateWithOneIndex(indexName, 5, 1, false).build();
        ClusterState currentState = generateClusterStateWithOneIndex(indexName, 5, 2, true).build();

        RoutingTableIncrementalDiff routingTableIncrementalDiff = new RoutingTableIncrementalDiff(
            previousState.getRoutingTable(),
            currentState.getRoutingTable()
        );

        RemoteRoutingTableDiff remoteDiffForUpload = new RemoteRoutingTableDiff(
            routingTableIncrementalDiff,
            clusterUUID,
            compressor,
            STATE_TERM,
            STATE_VERSION
        );
        assertThat(remoteDiffForUpload.getFullBlobName(), nullValue());

        RemoteRoutingTableDiff remoteDiffForDownload = new RemoteRoutingTableDiff(TEST_BLOB_NAME, clusterUUID, compressor);
        assertThat(remoteDiffForDownload.getFullBlobName(), is(TEST_BLOB_NAME));
    }

    public void testBlobFileName() {
        String indexName = randomAlphaOfLength(randomIntBetween(1, 50));
        ClusterState previousState = generateClusterStateWithOneIndex(indexName, 5, 1, false).build();
        ClusterState currentState = generateClusterStateWithOneIndex(indexName, 5, 2, true).build();

        RoutingTableIncrementalDiff routingTableIncrementalDiff = new RoutingTableIncrementalDiff(
            previousState.getRoutingTable(),
            currentState.getRoutingTable()
        );

        RemoteRoutingTableDiff remoteDiffForUpload = new RemoteRoutingTableDiff(
            routingTableIncrementalDiff,
            clusterUUID,
            compressor,
            STATE_TERM,
            STATE_VERSION
        );
        assertThat(remoteDiffForUpload.getBlobFileName(), nullValue());

        RemoteRoutingTableDiff remoteDiffForDownload = new RemoteRoutingTableDiff(TEST_BLOB_NAME, clusterUUID, compressor);
        assertThat(remoteDiffForDownload.getBlobFileName(), is(TEST_BLOB_FILE_NAME));
    }

    public void testBlobPathParameters() {
        String indexName = randomAlphaOfLength(randomIntBetween(1, 50));
        ClusterState previousState = generateClusterStateWithOneIndex(indexName, 5, 1, false).build();
        ClusterState currentState = generateClusterStateWithOneIndex(indexName, 5, 2, true).build();

        RoutingTableIncrementalDiff routingTableIncrementalDiff = new RoutingTableIncrementalDiff(
            previousState.getRoutingTable(),
            currentState.getRoutingTable()
        );

        RemoteRoutingTableDiff remoteDiffForUpload = new RemoteRoutingTableDiff(
            routingTableIncrementalDiff,
            clusterUUID,
            compressor,
            STATE_TERM,
            STATE_VERSION
        );
        assertThat(remoteDiffForUpload.getBlobFileName(), nullValue());

        BlobPathParameters params = remoteDiffForUpload.getBlobPathParameters();
        assertThat(params.getPathTokens(), is(List.of(ROUTING_TABLE_DIFF_PATH_TOKEN)));
        String expectedPrefix = ROUTING_TABLE_DIFF_METADATA_PREFIX;
        assertThat(params.getFilePrefix(), is(expectedPrefix));
    }

    public void testGenerateBlobFileName() {
        String indexName = randomAlphaOfLength(randomIntBetween(1, 50));
        ClusterState previousState = generateClusterStateWithOneIndex(indexName, 5, 1, false).build();
        ClusterState currentState = generateClusterStateWithOneIndex(indexName, 5, 2, true).build();

        RoutingTableIncrementalDiff routingTableIncrementalDiff = new RoutingTableIncrementalDiff(
            previousState.getRoutingTable(),
            currentState.getRoutingTable()
        );

        RemoteRoutingTableDiff remoteDiffForUpload = new RemoteRoutingTableDiff(
            routingTableIncrementalDiff,
            clusterUUID,
            compressor,
            STATE_TERM,
            STATE_VERSION
        );

        String blobFileName = remoteDiffForUpload.generateBlobFileName();
        String[] nameTokens = blobFileName.split("__");
        assertEquals(ROUTING_TABLE_DIFF_METADATA_PREFIX, nameTokens[0]);
        assertEquals(RemoteStoreUtils.invertLong(STATE_TERM), nameTokens[1]);
        assertEquals(RemoteStoreUtils.invertLong(STATE_VERSION), nameTokens[2]);
        assertThat(RemoteStoreUtils.invertLong(nameTokens[3]), lessThanOrEqualTo(System.currentTimeMillis()));
    }

    public void testGetUploadedMetadata() throws IOException {
        String indexName = randomAlphaOfLength(randomIntBetween(1, 50));
        ClusterState previousState = generateClusterStateWithOneIndex(indexName, 5, 1, false).build();
        ClusterState currentState = generateClusterStateWithOneIndex(indexName, 5, 2, true).build();

        RoutingTableIncrementalDiff routingTableIncrementalDiff = new RoutingTableIncrementalDiff(
            previousState.getRoutingTable(),
            currentState.getRoutingTable()
        );

        RemoteRoutingTableDiff remoteDiffForUpload = new RemoteRoutingTableDiff(
            routingTableIncrementalDiff,
            clusterUUID,
            compressor,
            STATE_TERM,
            STATE_VERSION
        );

        remoteDiffForUpload.setFullBlobName(new BlobPath().add(TEST_BLOB_PATH));
        ClusterMetadataManifest.UploadedMetadata uploadedMetadataAttribute = remoteDiffForUpload.getUploadedMetadata();
        assertEquals(ROUTING_TABLE_DIFF_FILE, uploadedMetadataAttribute.getComponent());
    }

    public void testStreamOperations() throws IOException {
        String indexName = randomAlphaOfLength(randomIntBetween(1, 50));
        int numberOfShards = randomIntBetween(1, 10);
        int numberOfReplicas = randomIntBetween(1, 10);

        ClusterState previousState = generateClusterStateWithOneIndex(indexName, numberOfShards, numberOfReplicas, false).build();
        ClusterState currentState = generateClusterStateWithOneIndex(indexName, numberOfShards, numberOfReplicas + 1, true).build();

        RoutingTableIncrementalDiff routingTableIncrementalDiff = new RoutingTableIncrementalDiff(
            previousState.getRoutingTable(),
            currentState.getRoutingTable()
        );

        RemoteRoutingTableDiff remoteDiffForUpload = new RemoteRoutingTableDiff(
            routingTableIncrementalDiff,
            clusterUUID,
            compressor,
            STATE_TERM,
            STATE_VERSION
        );

        // Serialize the remote diff
        InputStream inputStream = remoteDiffForUpload.serialize();

        // Create a new instance for deserialization
        RemoteRoutingTableDiff remoteDiffForDownload = new RemoteRoutingTableDiff(TEST_BLOB_NAME, clusterUUID, compressor);

        // Deserialize the remote diff
        Diff<RoutingTable> deserializedDiff = remoteDiffForDownload.deserialize(inputStream);

        // Assert that the indices routing table created from routingTableIncrementalDiff and deserializedDiff is equal
        assertEquals(
            routingTableIncrementalDiff.apply(previousState.getRoutingTable()).getIndicesRouting(),
            deserializedDiff.apply(previousState.getRoutingTable()).getIndicesRouting()
        );
    }
}
