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
import org.density.cluster.metadata.IndexTemplateMetadata;
import org.density.cluster.metadata.Metadata;
import org.density.cluster.metadata.TemplatesMetadata;
import org.density.common.blobstore.BlobPath;
import org.density.common.compress.DeflateCompressor;
import org.density.common.network.NetworkModule;
import org.density.common.settings.ClusterSettings;
import org.density.common.settings.Settings;
import org.density.core.compress.Compressor;
import org.density.core.compress.NoneCompressor;
import org.density.core.xcontent.NamedXContentRegistry;
import org.density.gateway.remote.RemoteClusterStateUtils;
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
import java.util.Arrays;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.density.gateway.remote.model.RemoteGlobalMetadata.GLOBAL_METADATA_FORMAT;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RemoteGlobalMetadataTests extends DensityTestCase {

    private static final String TEST_BLOB_NAME = "/test-path/test-blob-name";
    private static final String TEST_BLOB_FILE_NAME = "test-blob-name";
    private static final long TERM = 3L;
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
        RemoteGlobalMetadata remoteObjectForDownload = new RemoteGlobalMetadata(
            TEST_BLOB_NAME,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        assertThat(remoteObjectForDownload.clusterUUID(), is(clusterUUID));
    }

    public void testFullBlobName() {
        RemoteGlobalMetadata remoteObjectForDownload = new RemoteGlobalMetadata(
            TEST_BLOB_NAME,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        assertThat(remoteObjectForDownload.getFullBlobName(), is(TEST_BLOB_NAME));
    }

    public void testBlobFileName() {
        RemoteGlobalMetadata remoteObjectForDownload = new RemoteGlobalMetadata(
            TEST_BLOB_NAME,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        assertThat(remoteObjectForDownload.getBlobFileName(), is(TEST_BLOB_FILE_NAME));
    }

    public void testBlobPathTokens() {
        String uploadedFile = "user/local/density/globalMetadata";
        RemoteGlobalMetadata remoteObjectForDownload = new RemoteGlobalMetadata(
            uploadedFile,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        assertThat(remoteObjectForDownload.getBlobPathTokens(), is(new String[] { "user", "local", "density", "globalMetadata" }));
    }

    public void testBlobPathParameters() {
        RemoteGlobalMetadata remoteObjectForDownload = new RemoteGlobalMetadata(
            TEST_BLOB_NAME,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        assertThrows(UnsupportedOperationException.class, remoteObjectForDownload::getBlobPathParameters);
    }

    public void testGenerateBlobFileName() {
        RemoteGlobalMetadata remoteObjectForDownload = new RemoteGlobalMetadata(
            TEST_BLOB_NAME,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        assertThrows(UnsupportedOperationException.class, remoteObjectForDownload::generateBlobFileName);
    }

    public void testGetUploadedMetadata() {
        RemoteGlobalMetadata remoteObjectForDownload = new RemoteGlobalMetadata(
            TEST_BLOB_NAME,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        assertThrows(UnsupportedOperationException.class, remoteObjectForDownload::getUploadedMetadata);
    }

    public void testSerDe() throws IOException {
        Metadata globalMetadata = getGlobalMetadata();
        try (
            InputStream inputStream = GLOBAL_METADATA_FORMAT.serialize(
                globalMetadata,
                TEST_BLOB_FILE_NAME,
                compressor,
                RemoteClusterStateUtils.FORMAT_PARAMS
            ).streamInput()
        ) {
            RemoteGlobalMetadata remoteObjectForDownload = new RemoteGlobalMetadata(
                TEST_BLOB_NAME,
                clusterUUID,
                compressor,
                namedXContentRegistry
            );
            assertThat(inputStream.available(), greaterThan(0));
            Metadata readglobalMetadata = remoteObjectForDownload.deserialize(inputStream);
            assertTrue(Metadata.isGlobalStateEquals(readglobalMetadata, globalMetadata));
        }
    }

    public static Metadata getGlobalMetadata() {
        return Metadata.builder()
            .templates(
                TemplatesMetadata.builder()
                    .put(
                        IndexTemplateMetadata.builder("template" + randomAlphaOfLength(3))
                            .patterns(Arrays.asList("bar-*", "foo-*"))
                            .settings(
                                Settings.builder()
                                    .put("index.random_index_setting_" + randomAlphaOfLength(3), randomAlphaOfLength(5))
                                    .build()
                            )
                            .build()
                    )
                    .build()
            )
            .coordinationMetadata(
                CoordinationMetadata.builder()
                    .term(TERM)
                    .lastAcceptedConfiguration(new VotingConfiguration(Set.of("node1")))
                    .lastCommittedConfiguration(new VotingConfiguration(Set.of("node1")))
                    .addVotingConfigExclusion(new VotingConfigExclusion("node2", " node-2"))
                    .build()
            )
            .build();
    }
}
