/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.gateway.remote.model;

import org.density.Version;
import org.density.cluster.ClusterModule;
import org.density.cluster.metadata.IndexMetadata;
import org.density.common.blobstore.BlobPath;
import org.density.common.compress.DeflateCompressor;
import org.density.common.network.NetworkModule;
import org.density.common.remote.BlobPathParameters;
import org.density.common.settings.ClusterSettings;
import org.density.common.settings.Settings;
import org.density.core.compress.Compressor;
import org.density.core.compress.NoneCompressor;
import org.density.core.index.Index;
import org.density.core.xcontent.NamedXContentRegistry;
import org.density.gateway.remote.ClusterMetadataManifest.UploadedMetadata;
import org.density.gateway.remote.RemoteClusterStateUtils;
import org.density.index.remote.RemoteStoreEnums.PathHashAlgorithm;
import org.density.index.remote.RemoteStoreEnums.PathType;
import org.density.index.remote.RemoteStoreUtils;
import org.density.index.translog.transfer.BlobStoreTransferService;
import org.density.indices.IndicesModule;
import org.density.repositories.blobstore.BlobStoreRepository;
import org.density.test.DensityTestCase;
import org.density.threadpool.TestThreadPool;
import org.density.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.density.gateway.remote.model.RemoteIndexMetadata.INDEX;
import static org.density.gateway.remote.model.RemoteIndexMetadata.INDEX_METADATA_CURRENT_CODEC_VERSION;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RemoteIndexMetadataTests extends DensityTestCase {

    private static final String TEST_BLOB_NAME = "/test-path/test-blob-name";
    private static final String TEST_BLOB_PATH = "test-path";
    private static final String TEST_BLOB_FILE_NAME = "test-blob-name";
    private static final long VERSION = 5L;

    private String clusterUUID;
    private BlobStoreTransferService blobStoreTransferService;
    private BlobStoreRepository blobStoreRepository;
    private String clusterName;
    private ClusterSettings clusterSettings;
    private Compressor compressor;
    private NamedXContentRegistry namedXContentRegistry;
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
        this.clusterName = "test-cluster-name";
        compressor = new NoneCompressor();
        namedXContentRegistry = new NamedXContentRegistry(
            Stream.of(
                NetworkModule.getNamedXContents().stream(),
                IndicesModule.getNamedXContents().stream(),
                ClusterModule.getNamedXWriteables().stream()
            ).flatMap(Function.identity()).collect(toList())
        );
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdown();
    }

    public void testClusterUUID() {
        IndexMetadata indexMetadata = getIndexMetadata();
        RemoteIndexMetadata remoteObjectForUpload = new RemoteIndexMetadata(
            indexMetadata,
            clusterUUID,
            compressor,
            namedXContentRegistry,
            null,
            null,
            null
        );
        assertThat(remoteObjectForUpload.clusterUUID(), is(clusterUUID));

        RemoteIndexMetadata remoteObjectForDownload = new RemoteIndexMetadata(
            TEST_BLOB_NAME,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        assertThat(remoteObjectForDownload.clusterUUID(), is(clusterUUID));
    }

    public void testFullBlobName() {
        IndexMetadata indexMetadata = getIndexMetadata();
        RemoteIndexMetadata remoteObjectForUpload = new RemoteIndexMetadata(
            indexMetadata,
            clusterUUID,
            compressor,
            namedXContentRegistry,
            null,
            null,
            null
        );
        assertThat(remoteObjectForUpload.getFullBlobName(), nullValue());

        RemoteIndexMetadata remoteObjectForDownload = new RemoteIndexMetadata(
            TEST_BLOB_NAME,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        assertThat(remoteObjectForDownload.getFullBlobName(), is(TEST_BLOB_NAME));
    }

    public void testBlobFileName() {
        IndexMetadata indexMetadata = getIndexMetadata();
        RemoteIndexMetadata remoteObjectForUpload = new RemoteIndexMetadata(
            indexMetadata,
            clusterUUID,
            compressor,
            namedXContentRegistry,
            null,
            null,
            null
        );
        assertThat(remoteObjectForUpload.getBlobFileName(), nullValue());

        RemoteIndexMetadata remoteObjectForDownload = new RemoteIndexMetadata(
            TEST_BLOB_NAME,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        assertThat(remoteObjectForDownload.getBlobFileName(), is(TEST_BLOB_FILE_NAME));
    }

    public void testBlobPathParameters() {
        IndexMetadata indexMetadata = getIndexMetadata();
        RemoteIndexMetadata remoteObjectForUpload = new RemoteIndexMetadata(
            indexMetadata,
            clusterUUID,
            compressor,
            namedXContentRegistry,
            null,
            null,
            null
        );
        BlobPathParameters params = remoteObjectForUpload.getBlobPathParameters();
        assertThat(params.getPathTokens(), is(List.of(INDEX, indexMetadata.getIndexUUID())));
        assertThat(params.getFilePrefix(), is("metadata"));
    }

    public void testGenerateBlobFileName() {
        IndexMetadata indexMetadata = getIndexMetadata();
        RemoteIndexMetadata remoteObjectForUpload = new RemoteIndexMetadata(
            indexMetadata,
            clusterUUID,
            compressor,
            namedXContentRegistry,
            null,
            null,
            null
        );
        String blobFileName = remoteObjectForUpload.generateBlobFileName();
        String[] nameTokens = blobFileName.split(RemoteClusterStateUtils.DELIMITER);
        assertThat(nameTokens[0], is("metadata"));
        assertThat(RemoteStoreUtils.invertLong(nameTokens[1]), is(VERSION));
        assertThat(RemoteStoreUtils.invertLong(nameTokens[2]), lessThanOrEqualTo(System.currentTimeMillis()));
        assertThat(nameTokens[3], is(String.valueOf(INDEX_METADATA_CURRENT_CODEC_VERSION)));
    }

    public void testGetUploadedMetadata() throws IOException {
        IndexMetadata indexMetadata = getIndexMetadata();
        RemoteIndexMetadata remoteObjectForUpload = new RemoteIndexMetadata(
            indexMetadata,
            clusterUUID,
            compressor,
            namedXContentRegistry,
            null,
            null,
            null
        );
        assertThrows(AssertionError.class, remoteObjectForUpload::getUploadedMetadata);
        remoteObjectForUpload.setFullBlobName(new BlobPath().add(TEST_BLOB_PATH));
        UploadedMetadata uploadedMetadata = remoteObjectForUpload.getUploadedMetadata();
        assertEquals(uploadedMetadata.getUploadedFilename(), remoteObjectForUpload.getFullBlobName());
    }

    public void testSerDe() throws IOException {
        IndexMetadata indexMetadata = getIndexMetadata();
        RemoteIndexMetadata remoteObjectForUpload = new RemoteIndexMetadata(
            indexMetadata,
            clusterUUID,
            compressor,
            namedXContentRegistry,
            null,
            null,
            null
        );
        try (InputStream inputStream = remoteObjectForUpload.serialize()) {
            assertThat(inputStream.available(), greaterThan(0));
            IndexMetadata readIndexMetadata = remoteObjectForUpload.deserialize(inputStream);
            assertThat(readIndexMetadata, is(indexMetadata));
        }
    }

    public void testPrefixedPath() {
        IndexMetadata indexMetadata = getIndexMetadata();
        String fixedPrefix = "*";
        RemoteIndexMetadata remoteObjectForUpload = new RemoteIndexMetadata(
            indexMetadata,
            clusterUUID,
            compressor,
            namedXContentRegistry,
            PathType.HASHED_PREFIX,
            PathHashAlgorithm.FNV_1A_COMPOSITE_1,
            fixedPrefix
        );
        String testPath = "test-path";
        String expectedPath = fixedPrefix + "410100110100101/test-path/index-uuid/";
        BlobPath prefixedPath = remoteObjectForUpload.getPrefixedPath(BlobPath.cleanPath().add(testPath));
        assertThat(prefixedPath.buildAsString(), is(expectedPath));

    }

    private IndexMetadata getIndexMetadata() {
        final Index index = new Index("test-index", "index-uuid");
        final Settings idxSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
            .build();
        return new IndexMetadata.Builder(index.getName()).settings(idxSettings)
            .version(VERSION)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
    }
}
