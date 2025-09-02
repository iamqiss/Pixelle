/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.gateway.remote;

import org.density.cluster.ClusterModule;
import org.density.cluster.ClusterState;
import org.density.common.blobstore.BlobContainer;
import org.density.common.blobstore.BlobPath;
import org.density.common.blobstore.BlobStore;
import org.density.common.network.NetworkModule;
import org.density.common.settings.ClusterSettings;
import org.density.common.settings.Settings;
import org.density.core.compress.Compressor;
import org.density.core.compress.NoneCompressor;
import org.density.core.xcontent.NamedXContentRegistry;
import org.density.index.translog.transfer.BlobStoreTransferService;
import org.density.indices.IndicesModule;
import org.density.repositories.blobstore.BlobStoreRepository;
import org.density.test.DensityTestCase;
import org.density.threadpool.TestThreadPool;
import org.density.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.density.gateway.remote.RemoteClusterStateUtils.DELIMITER;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RemoteManifestManagerTests extends DensityTestCase {
    private RemoteManifestManager remoteManifestManager;
    private ClusterSettings clusterSettings;
    private BlobStoreRepository blobStoreRepository;
    private BlobStore blobStore;
    private BlobStoreTransferService blobStoreTransferService;
    private ThreadPool threadPool;

    @Before
    public void setup() {
        clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        blobStoreRepository = mock(BlobStoreRepository.class);
        NamedXContentRegistry xContentRegistry = new NamedXContentRegistry(
            Stream.of(
                NetworkModule.getNamedXContents().stream(),
                IndicesModule.getNamedXContents().stream(),
                ClusterModule.getNamedXWriteables().stream()
            ).flatMap(Function.identity()).collect(toList())
        );
        blobStoreTransferService = mock(BlobStoreTransferService.class);
        blobStore = mock(BlobStore.class);
        when(blobStoreRepository.blobStore()).thenReturn(blobStore);
        threadPool = new TestThreadPool("test");
        Compressor compressor = new NoneCompressor();
        when(blobStoreRepository.getCompressor()).thenReturn(compressor);
        when(blobStoreRepository.getNamedXContentRegistry()).thenReturn(xContentRegistry);
        remoteManifestManager = new RemoteManifestManager(
            clusterSettings,
            "test-cluster-name",
            "test-node-id",
            blobStoreRepository,
            blobStoreTransferService,
            threadPool
        );
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdown();
    }

    public void testMetadataManifestUploadWaitTimeSetting() {
        // verify default value
        assertEquals(
            RemoteManifestManager.METADATA_MANIFEST_UPLOAD_TIMEOUT_DEFAULT,
            remoteManifestManager.getMetadataManifestUploadTimeout()
        );

        // verify update metadata manifest upload timeout
        int metadataManifestUploadTimeout = randomIntBetween(1, 10);
        Settings newSettings = Settings.builder()
            .put("cluster.remote_store.state.metadata_manifest.upload_timeout", metadataManifestUploadTimeout + "s")
            .build();
        clusterSettings.applySettings(newSettings);
        assertEquals(metadataManifestUploadTimeout, remoteManifestManager.getMetadataManifestUploadTimeout().seconds());
    }

    public void testReadLatestMetadataManifestFailedIOException() throws IOException {
        final ClusterState clusterState = RemoteClusterStateServiceTests.generateClusterStateWithOneIndex()
            .nodes(RemoteClusterStateServiceTests.nodesWithLocalNodeClusterManager())
            .build();

        BlobContainer blobContainer = mockBlobStoreObjects();
        when(blobContainer.listBlobsByPrefixInSortedOrder("manifest" + DELIMITER, 1, BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC))
            .thenThrow(IOException.class);

        Exception e = assertThrows(
            IllegalStateException.class,
            () -> remoteManifestManager.getLatestClusterMetadataManifest(
                clusterState.getClusterName().value(),
                clusterState.metadata().clusterUUID()
            )
        );
        assertEquals(e.getMessage(), "Error while fetching latest manifest file for remote cluster state");
    }

    private BlobContainer mockBlobStoreObjects() {
        final BlobPath blobPath = mock(BlobPath.class);
        when((blobStoreRepository.basePath())).thenReturn(blobPath);
        when(blobPath.add(anyString())).thenReturn(blobPath);
        when(blobPath.buildAsString()).thenReturn("/blob/path/");
        final BlobContainer blobContainer = mock(BlobContainer.class);
        when(blobContainer.path()).thenReturn(blobPath);
        when(blobStore.blobContainer(any())).thenReturn(blobContainer);
        return blobContainer;
    }
}
