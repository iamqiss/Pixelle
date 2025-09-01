/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.gateway.remote.routingtable;

import org.density.Version;
import org.density.cluster.metadata.IndexMetadata;
import org.density.cluster.metadata.Metadata;
import org.density.cluster.routing.IndexRoutingTable;
import org.density.cluster.routing.RoutingTable;
import org.density.common.blobstore.BlobPath;
import org.density.common.compress.DeflateCompressor;
import org.density.common.remote.BlobPathParameters;
import org.density.common.settings.ClusterSettings;
import org.density.common.settings.Settings;
import org.density.core.common.io.stream.NamedWriteableRegistry;
import org.density.core.compress.Compressor;
import org.density.core.compress.NoneCompressor;
import org.density.gateway.remote.ClusterMetadataManifest;
import org.density.gateway.remote.RemoteClusterStateUtils;
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

import static org.density.gateway.remote.routingtable.RemoteIndexRoutingTable.INDEX_ROUTING_FILE;
import static org.density.gateway.remote.routingtable.RemoteIndexRoutingTable.INDEX_ROUTING_METADATA_PREFIX;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RemoteIndexRoutingTableTests extends DensityTestCase {

    private static final String TEST_BLOB_NAME = "/test-path/test-blob-name";
    private static final String TEST_BLOB_PATH = "test-path";
    private static final String TEST_BLOB_FILE_NAME = "test-blob-name";
    private static final String INDEX_ROUTING_TABLE_TYPE = "test-index-routing-table";
    private static final long STATE_VERSION = 3L;
    private static final long STATE_TERM = 2L;
    private String clusterUUID;
    private BlobStoreTransferService blobStoreTransferService;
    private BlobStoreRepository blobStoreRepository;
    private String clusterName;
    private ClusterSettings clusterSettings;
    private Compressor compressor;
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
        int numberOfShards = randomIntBetween(1, 10);
        int numberOfReplicas = randomIntBetween(1, 10);
        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder(indexName)
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(numberOfShards)
                    .numberOfReplicas(numberOfReplicas)
            )
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder().addAsNew(metadata.index(indexName)).build();

        initialRoutingTable.getIndicesRouting().values().forEach(indexRoutingTable -> {
            RemoteIndexRoutingTable remoteObjectForUpload = new RemoteIndexRoutingTable(
                indexRoutingTable,
                clusterUUID,
                compressor,
                STATE_TERM,
                STATE_VERSION
            );
            assertEquals(remoteObjectForUpload.clusterUUID(), clusterUUID);

            RemoteIndexRoutingTable remoteObjectForDownload = new RemoteIndexRoutingTable(TEST_BLOB_NAME, clusterUUID, compressor);
            assertEquals(remoteObjectForDownload.clusterUUID(), clusterUUID);
        });
    }

    public void testFullBlobName() {
        String indexName = randomAlphaOfLength(randomIntBetween(1, 50));
        int numberOfShards = randomIntBetween(1, 10);
        int numberOfReplicas = randomIntBetween(1, 10);
        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder(indexName)
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(numberOfShards)
                    .numberOfReplicas(numberOfReplicas)
            )
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder().addAsNew(metadata.index(indexName)).build();

        initialRoutingTable.getIndicesRouting().values().forEach(indexRoutingTable -> {
            RemoteIndexRoutingTable remoteObjectForUpload = new RemoteIndexRoutingTable(
                indexRoutingTable,
                clusterUUID,
                compressor,
                STATE_TERM,
                STATE_VERSION
            );
            assertThat(remoteObjectForUpload.getFullBlobName(), nullValue());

            RemoteIndexRoutingTable remoteObjectForDownload = new RemoteIndexRoutingTable(TEST_BLOB_NAME, clusterUUID, compressor);
            assertThat(remoteObjectForDownload.getFullBlobName(), is(TEST_BLOB_NAME));
        });
    }

    public void testBlobFileName() {
        String indexName = randomAlphaOfLength(randomIntBetween(1, 50));
        int numberOfShards = randomIntBetween(1, 10);
        int numberOfReplicas = randomIntBetween(1, 10);
        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder(indexName)
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(numberOfShards)
                    .numberOfReplicas(numberOfReplicas)
            )
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder().addAsNew(metadata.index(indexName)).build();

        initialRoutingTable.getIndicesRouting().values().forEach(indexRoutingTable -> {
            RemoteIndexRoutingTable remoteObjectForUpload = new RemoteIndexRoutingTable(
                indexRoutingTable,
                clusterUUID,
                compressor,
                STATE_TERM,
                STATE_VERSION
            );
            assertThat(remoteObjectForUpload.getBlobFileName(), nullValue());

            RemoteIndexRoutingTable remoteObjectForDownload = new RemoteIndexRoutingTable(TEST_BLOB_NAME, clusterUUID, compressor);
            assertThat(remoteObjectForDownload.getBlobFileName(), is(TEST_BLOB_FILE_NAME));
        });
    }

    public void testBlobPathTokens() {
        String uploadedFile = "user/local/density/routingTable";
        RemoteIndexRoutingTable remoteObjectForDownload = new RemoteIndexRoutingTable(uploadedFile, clusterUUID, compressor);
        assertThat(remoteObjectForDownload.getBlobPathTokens(), is(new String[] { "user", "local", "density", "routingTable" }));
    }

    public void testBlobPathParameters() {
        String indexName = randomAlphaOfLength(randomIntBetween(1, 50));
        int numberOfShards = randomIntBetween(1, 10);
        int numberOfReplicas = randomIntBetween(1, 10);
        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder(indexName)
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(numberOfShards)
                    .numberOfReplicas(numberOfReplicas)
            )
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder().addAsNew(metadata.index(indexName)).build();

        initialRoutingTable.getIndicesRouting().values().forEach(indexRoutingTable -> {
            RemoteIndexRoutingTable remoteObjectForUpload = new RemoteIndexRoutingTable(
                indexRoutingTable,
                clusterUUID,
                compressor,
                STATE_TERM,
                STATE_VERSION
            );
            assertThat(remoteObjectForUpload.getBlobFileName(), nullValue());

            BlobPathParameters params = remoteObjectForUpload.getBlobPathParameters();
            assertThat(params.getPathTokens(), is(List.of(indexRoutingTable.getIndex().getUUID())));
            String expectedPrefix = INDEX_ROUTING_FILE;
            assertThat(params.getFilePrefix(), is(expectedPrefix));
        });
    }

    public void testGenerateBlobFileName() {
        String indexName = randomAlphaOfLength(randomIntBetween(1, 50));
        int numberOfShards = randomIntBetween(1, 10);
        int numberOfReplicas = randomIntBetween(1, 10);
        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder(indexName)
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(numberOfShards)
                    .numberOfReplicas(numberOfReplicas)
            )
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder().addAsNew(metadata.index(indexName)).build();

        initialRoutingTable.getIndicesRouting().values().forEach(indexRoutingTable -> {
            RemoteIndexRoutingTable remoteObjectForUpload = new RemoteIndexRoutingTable(
                indexRoutingTable,
                clusterUUID,
                compressor,
                STATE_TERM,
                STATE_VERSION
            );

            String blobFileName = remoteObjectForUpload.generateBlobFileName();
            String[] nameTokens = blobFileName.split(RemoteClusterStateUtils.DELIMITER);
            assertEquals(nameTokens[0], INDEX_ROUTING_FILE);
            assertEquals(nameTokens[1], RemoteStoreUtils.invertLong(STATE_TERM));
            assertEquals(nameTokens[2], RemoteStoreUtils.invertLong(STATE_VERSION));
            assertThat(RemoteStoreUtils.invertLong(nameTokens[3]), lessThanOrEqualTo(System.currentTimeMillis()));
        });
    }

    public void testGetUploadedMetadata() throws IOException {
        String indexName = randomAlphaOfLength(randomIntBetween(1, 50));
        int numberOfShards = randomIntBetween(1, 10);
        int numberOfReplicas = randomIntBetween(1, 10);
        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder(indexName)
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(numberOfShards)
                    .numberOfReplicas(numberOfReplicas)
            )
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder().addAsNew(metadata.index(indexName)).build();

        initialRoutingTable.getIndicesRouting().values().forEach(indexRoutingTable -> {
            RemoteIndexRoutingTable remoteObjectForUpload = new RemoteIndexRoutingTable(
                indexRoutingTable,
                clusterUUID,
                compressor,
                STATE_TERM,
                STATE_VERSION
            );

            assertThrows(AssertionError.class, remoteObjectForUpload::getUploadedMetadata);

            try (InputStream inputStream = remoteObjectForUpload.serialize()) {
                remoteObjectForUpload.setFullBlobName(new BlobPath().add(TEST_BLOB_PATH));
                ClusterMetadataManifest.UploadedMetadata uploadedMetadata = remoteObjectForUpload.getUploadedMetadata();
                String expectedPrefix = INDEX_ROUTING_METADATA_PREFIX + indexRoutingTable.getIndex().getName();
                assertThat(uploadedMetadata.getComponent(), is(expectedPrefix));
                assertThat(uploadedMetadata.getUploadedFilename(), is(remoteObjectForUpload.getFullBlobName()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public void testSerDe() throws IOException {
        String indexName = randomAlphaOfLength(randomIntBetween(1, 50));
        int numberOfShards = randomIntBetween(1, 10);
        int numberOfReplicas = randomIntBetween(1, 10);
        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder(indexName)
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(numberOfShards)
                    .numberOfReplicas(numberOfReplicas)
            )
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder().addAsNew(metadata.index(indexName)).build();

        initialRoutingTable.getIndicesRouting().values().forEach(indexRoutingTable -> {
            RemoteIndexRoutingTable remoteObjectForUpload = new RemoteIndexRoutingTable(
                indexRoutingTable,
                clusterUUID,
                compressor,
                STATE_TERM,
                STATE_VERSION
            );

            assertThrows(AssertionError.class, remoteObjectForUpload::getUploadedMetadata);

            try (InputStream inputStream = remoteObjectForUpload.serialize()) {
                remoteObjectForUpload.setFullBlobName(BlobPath.cleanPath());
                assertThat(inputStream.available(), greaterThan(0));
                IndexRoutingTable readIndexRoutingTable = remoteObjectForUpload.deserialize(inputStream);
                assertEquals(readIndexRoutingTable, indexRoutingTable);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
