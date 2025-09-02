/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.gateway.remote.model;

import org.density.cluster.ClusterModule;
import org.density.cluster.coordination.CoordinationMetadata;
import org.density.cluster.coordination.CoordinationMetadata.VotingConfigExclusion;
import org.density.cluster.coordination.CoordinationMetadata.VotingConfiguration;
import org.density.common.blobstore.BlobPath;
import org.density.common.compress.DeflateCompressor;
import org.density.common.network.NetworkModule;
import org.density.common.remote.BlobPathParameters;
import org.density.common.settings.ClusterSettings;
import org.density.common.settings.Settings;
import org.density.core.compress.Compressor;
import org.density.core.compress.NoneCompressor;
import org.density.core.xcontent.NamedXContentRegistry;
import org.density.gateway.remote.ClusterMetadataManifest.UploadedMetadata;
import org.density.gateway.remote.RemoteClusterStateUtils;
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
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.density.gateway.remote.RemoteClusterStateUtils.GLOBAL_METADATA_CURRENT_CODEC_VERSION;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RemoteCoordinationMetadataTests extends DensityTestCase {
    private static final String TEST_BLOB_NAME = "/test-path/test-blob-name";
    private static final String TEST_BLOB_PATH = "test-path";
    private static final String TEST_BLOB_FILE_NAME = "test-blob-name";
    private static final long TERM = 3L;
    private static final long METADATA_VERSION = 3L;
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
        compressor = new NoneCompressor();
        namedXContentRegistry = new NamedXContentRegistry(
            Stream.of(
                NetworkModule.getNamedXContents().stream(),
                IndicesModule.getNamedXContents().stream(),
                ClusterModule.getNamedXWriteables().stream()
            ).flatMap(Function.identity()).collect(toList())
        );
        this.clusterName = "test-cluster-name";
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdown();
    }

    public void testClusterUUID() {
        CoordinationMetadata coordinationMetadata = getCoordinationMetadata();
        RemoteCoordinationMetadata remoteObjectForUpload = new RemoteCoordinationMetadata(
            coordinationMetadata,
            METADATA_VERSION,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        assertThat(remoteObjectForUpload.clusterUUID(), is(clusterUUID));

        RemoteCoordinationMetadata remoteObjectForDownload = new RemoteCoordinationMetadata(
            TEST_BLOB_NAME,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        assertThat(remoteObjectForDownload.clusterUUID(), is(clusterUUID));
    }

    public void testFullBlobName() {
        CoordinationMetadata coordinationMetadata = getCoordinationMetadata();
        RemoteCoordinationMetadata remoteObjectForUpload = new RemoteCoordinationMetadata(
            coordinationMetadata,
            METADATA_VERSION,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        assertThat(remoteObjectForUpload.getFullBlobName(), nullValue());

        RemoteCoordinationMetadata remoteObjectForDownload = new RemoteCoordinationMetadata(
            TEST_BLOB_NAME,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        assertThat(remoteObjectForDownload.getFullBlobName(), is(TEST_BLOB_NAME));
    }

    public void testBlobFileName() {
        CoordinationMetadata coordinationMetadata = getCoordinationMetadata();
        RemoteCoordinationMetadata remoteObjectForUpload = new RemoteCoordinationMetadata(
            coordinationMetadata,
            METADATA_VERSION,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        assertThat(remoteObjectForUpload.getBlobFileName(), nullValue());

        RemoteCoordinationMetadata remoteObjectForDownload = new RemoteCoordinationMetadata(
            TEST_BLOB_NAME,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        assertThat(remoteObjectForDownload.getBlobFileName(), is(TEST_BLOB_FILE_NAME));
    }

    public void testBlobPathTokens() {
        String uploadedFile = "user/local/density/coordinationMetadata";
        RemoteCoordinationMetadata remoteObjectForDownload = new RemoteCoordinationMetadata(
            uploadedFile,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        assertThat(remoteObjectForDownload.getBlobPathTokens(), is(new String[] { "user", "local", "density", "coordinationMetadata" }));
    }

    public void testBlobPathParameters() {
        CoordinationMetadata coordinationMetadata = getCoordinationMetadata();
        RemoteCoordinationMetadata remoteObjectForUpload = new RemoteCoordinationMetadata(
            coordinationMetadata,
            METADATA_VERSION,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        BlobPathParameters params = remoteObjectForUpload.getBlobPathParameters();
        assertThat(params.getPathTokens(), is(List.of(RemoteClusterStateUtils.GLOBAL_METADATA_PATH_TOKEN)));
        assertThat(params.getFilePrefix(), is(RemoteCoordinationMetadata.COORDINATION_METADATA));
    }

    public void testGenerateBlobFileName() {
        CoordinationMetadata coordinationMetadata = getCoordinationMetadata();
        RemoteCoordinationMetadata remoteObjectForUpload = new RemoteCoordinationMetadata(
            coordinationMetadata,
            METADATA_VERSION,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        String blobFileName = remoteObjectForUpload.generateBlobFileName();
        String[] nameTokens = blobFileName.split(RemoteClusterStateUtils.DELIMITER);
        assertThat(nameTokens[0], is(RemoteCoordinationMetadata.COORDINATION_METADATA));
        assertThat(RemoteStoreUtils.invertLong(nameTokens[1]), is(METADATA_VERSION));
        assertThat(RemoteStoreUtils.invertLong(nameTokens[2]), lessThanOrEqualTo(System.currentTimeMillis()));
        assertThat(nameTokens[3], is(String.valueOf(GLOBAL_METADATA_CURRENT_CODEC_VERSION)));

    }

    public void testGetUploadedMetadata() throws IOException {
        CoordinationMetadata coordinationMetadata = getCoordinationMetadata();
        RemoteCoordinationMetadata remoteObjectForUpload = new RemoteCoordinationMetadata(
            coordinationMetadata,
            METADATA_VERSION,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        assertThrows(AssertionError.class, remoteObjectForUpload::getUploadedMetadata);

        try (InputStream inputStream = remoteObjectForUpload.serialize()) {
            remoteObjectForUpload.setFullBlobName(new BlobPath().add(TEST_BLOB_PATH));
            UploadedMetadata uploadedMetadata = remoteObjectForUpload.getUploadedMetadata();
            assertThat(uploadedMetadata.getComponent(), is(RemoteCoordinationMetadata.COORDINATION_METADATA));
            assertThat(uploadedMetadata.getUploadedFilename(), is(remoteObjectForUpload.getFullBlobName()));
        }
    }

    public void testSerDe() throws IOException {
        CoordinationMetadata coordinationMetadata = getCoordinationMetadata();
        RemoteCoordinationMetadata remoteObjectForUpload = new RemoteCoordinationMetadata(
            coordinationMetadata,
            METADATA_VERSION,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        try (InputStream inputStream = remoteObjectForUpload.serialize()) {
            remoteObjectForUpload.setFullBlobName(BlobPath.cleanPath());
            assertThat(inputStream.available(), greaterThan(0));
            CoordinationMetadata readcoordinationMetadata = remoteObjectForUpload.deserialize(inputStream);
            assertThat(readcoordinationMetadata, is(coordinationMetadata));
        }
    }

    public static CoordinationMetadata getCoordinationMetadata() {
        return CoordinationMetadata.builder()
            .term(TERM)
            .lastAcceptedConfiguration(new VotingConfiguration(Set.of("node1")))
            .lastCommittedConfiguration(new VotingConfiguration(Set.of("node1")))
            .addVotingConfigExclusion(new VotingConfigExclusion("node2", " node-2"))
            .build();
    }
}
