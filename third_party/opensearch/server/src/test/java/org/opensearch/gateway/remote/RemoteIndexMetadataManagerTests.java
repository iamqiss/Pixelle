/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.gateway.remote;

import org.density.Version;
import org.density.action.LatchedActionListener;
import org.density.cluster.metadata.AliasMetadata;
import org.density.cluster.metadata.IndexMetadata;
import org.density.common.Nullable;
import org.density.common.blobstore.AsyncMultiStreamBlobContainer;
import org.density.common.blobstore.BlobContainer;
import org.density.common.blobstore.BlobPath;
import org.density.common.blobstore.BlobStore;
import org.density.common.blobstore.stream.write.WritePriority;
import org.density.common.settings.ClusterSettings;
import org.density.common.settings.Settings;
import org.density.common.util.TestCapturingListener;
import org.density.core.action.ActionListener;
import org.density.core.compress.Compressor;
import org.density.core.compress.NoneCompressor;
import org.density.gateway.remote.model.RemoteIndexMetadata;
import org.density.gateway.remote.model.RemoteReadResult;
import org.density.index.remote.RemoteStoreEnums;
import org.density.index.remote.RemoteStoreUtils;
import org.density.index.translog.transfer.BlobStoreTransferService;
import org.density.repositories.blobstore.BlobStoreRepository;
import org.density.test.DensityTestCase;
import org.density.threadpool.TestThreadPool;
import org.density.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import static org.density.gateway.remote.RemoteClusterStateService.FORMAT_PARAMS;
import static org.density.gateway.remote.RemoteClusterStateUtils.DELIMITER;
import static org.density.gateway.remote.RemoteClusterStateUtils.PATH_DELIMITER;
import static org.density.gateway.remote.model.RemoteIndexMetadata.INDEX;
import static org.density.gateway.remote.model.RemoteIndexMetadata.INDEX_METADATA_FORMAT;
import static org.density.index.remote.RemoteStoreEnums.PathHashAlgorithm.FNV_1A_BASE64;
import static org.density.index.remote.RemoteStoreEnums.PathType.HASHED_PREFIX;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyIterable;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RemoteIndexMetadataManagerTests extends DensityTestCase {

    private RemoteIndexMetadataManager remoteIndexMetadataManager;
    private BlobStoreRepository blobStoreRepository;
    private BlobStoreTransferService blobStoreTransferService;
    private Compressor compressor;
    private ClusterSettings clusterSettings;
    private final ThreadPool threadPool = new TestThreadPool(getClass().getName());

    @Before
    public void setup() {
        clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        blobStoreRepository = mock(BlobStoreRepository.class);
        BlobPath blobPath = new BlobPath().add("random-path");
        when((blobStoreRepository.basePath())).thenReturn(blobPath);
        blobStoreTransferService = mock(BlobStoreTransferService.class);
        compressor = new NoneCompressor();
        when(blobStoreRepository.getCompressor()).thenReturn(compressor);
        remoteIndexMetadataManager = new RemoteIndexMetadataManager(
            clusterSettings,
            "test-cluster",
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

    public void testGetAsyncWriteRunnable_Success() throws Exception {
        IndexMetadata indexMetadata = getIndexMetadata(randomAlphaOfLength(10), randomBoolean(), randomAlphaOfLength(10));
        BlobContainer blobContainer = mock(AsyncMultiStreamBlobContainer.class);
        BlobStore blobStore = mock(BlobStore.class);
        when(blobStore.blobContainer(any())).thenReturn(blobContainer);
        TestCapturingListener<ClusterMetadataManifest.UploadedMetadata> listener = new TestCapturingListener<>();
        CountDownLatch latch = new CountDownLatch(1);
        String expectedFilePrefix = String.join(DELIMITER, "metadata", RemoteStoreUtils.invertLong(indexMetadata.getVersion()));

        doAnswer((invocationOnMock -> {
            invocationOnMock.getArgument(4, ActionListener.class).onResponse(null);
            return null;
        })).when(blobStoreTransferService).uploadBlob(any(), any(), any(), eq(WritePriority.URGENT), any(ActionListener.class));

        remoteIndexMetadataManager.writeAsync(
            INDEX,
            new RemoteIndexMetadata(indexMetadata, "cluster-uuid", compressor, null, null, null, null),
            new LatchedActionListener<>(listener, latch)
        );
        latch.await();

        assertNull(listener.getFailure());
        assertNotNull(listener.getResult());
        ClusterMetadataManifest.UploadedMetadata uploadedMetadata = listener.getResult();
        assertEquals(INDEX + "--" + indexMetadata.getIndex().getName(), uploadedMetadata.getComponent());
        String uploadedFileName = uploadedMetadata.getUploadedFilename();
        String[] pathTokens = uploadedFileName.split(PATH_DELIMITER);
        assertEquals(7, pathTokens.length);
        assertEquals(INDEX, pathTokens[4]);
        assertEquals(indexMetadata.getIndex().getUUID(), pathTokens[5]);
        assertTrue(pathTokens[6].startsWith(expectedFilePrefix));
    }

    public void testGetAsyncWriteRunnable_IOFailure() throws Exception {
        IndexMetadata indexMetadata = getIndexMetadata(randomAlphaOfLength(10), randomBoolean(), randomAlphaOfLength(10));
        BlobContainer blobContainer = mock(AsyncMultiStreamBlobContainer.class);
        BlobStore blobStore = mock(BlobStore.class);
        when(blobStore.blobContainer(any())).thenReturn(blobContainer);
        TestCapturingListener<ClusterMetadataManifest.UploadedMetadata> listener = new TestCapturingListener<>();
        CountDownLatch latch = new CountDownLatch(1);

        doAnswer((invocationOnMock -> {
            invocationOnMock.getArgument(4, ActionListener.class).onFailure(new IOException("failure"));
            return null;
        })).when(blobStoreTransferService).uploadBlob(any(), any(), any(), eq(WritePriority.URGENT), any(ActionListener.class));

        remoteIndexMetadataManager.writeAsync(
            INDEX,
            new RemoteIndexMetadata(indexMetadata, "cluster-uuid", compressor, null, null, null, null),
            new LatchedActionListener<>(listener, latch)
        );
        latch.await();
        assertNull(listener.getResult());
        assertNotNull(listener.getFailure());
        assertTrue(listener.getFailure() instanceof RemoteStateTransferException);
    }

    public void testGetAsyncReadRunnable_Success() throws Exception {
        IndexMetadata indexMetadata = getIndexMetadata(randomAlphaOfLength(10), randomBoolean(), randomAlphaOfLength(10));
        String fileName = randomAlphaOfLength(10);
        fileName = fileName + DELIMITER + '2';
        when(blobStoreTransferService.downloadBlob(anyIterable(), anyString())).thenReturn(
            INDEX_METADATA_FORMAT.serialize(indexMetadata, fileName, compressor, FORMAT_PARAMS).streamInput()
        );
        TestCapturingListener<RemoteReadResult> listener = new TestCapturingListener<>();
        CountDownLatch latch = new CountDownLatch(1);

        remoteIndexMetadataManager.readAsync(
            INDEX,
            new RemoteIndexMetadata(fileName, "cluster-uuid", compressor, null),
            new LatchedActionListener<>(listener, latch)
        );
        latch.await();
        assertNull(listener.getFailure());
        assertNotNull(listener.getResult());
        assertEquals(indexMetadata, listener.getResult().getObj());
    }

    public void testGetAsyncReadRunnable_IOFailure() throws Exception {
        String fileName = randomAlphaOfLength(10);
        fileName = fileName + DELIMITER + '2';
        Exception exception = new IOException("testing failure");
        doThrow(exception).when(blobStoreTransferService).downloadBlob(anyIterable(), anyString());
        TestCapturingListener<RemoteReadResult> listener = new TestCapturingListener<>();
        CountDownLatch latch = new CountDownLatch(1);

        remoteIndexMetadataManager.readAsync(
            INDEX,
            new RemoteIndexMetadata(fileName, "cluster-uuid", compressor, null),
            new LatchedActionListener<>(listener, latch)
        );
        latch.await();
        assertNull(listener.getResult());
        assertNotNull(listener.getFailure());
        assertEquals(exception, listener.getFailure().getCause());
        assertTrue(listener.getFailure() instanceof RemoteStateTransferException);
    }

    public void testRemoteIndexMetadataPathTypeSetting() {
        // Assert the default is HASHED_PREFIX
        assertEquals(HASHED_PREFIX.toString(), remoteIndexMetadataManager.getPathTypeSetting().toString());

        Settings newSettings = Settings.builder()
            .put("cluster.remote_store.index_metadata.path_type", RemoteStoreEnums.PathType.FIXED.toString())
            .build();
        clusterSettings.applySettings(newSettings);
        assertEquals(RemoteStoreEnums.PathType.FIXED.toString(), remoteIndexMetadataManager.getPathTypeSetting().toString());
    }

    public void testRemoteIndexMetadataHashAlgoSetting() {
        // Assert the default is FNV_1A_BASE64
        assertEquals(FNV_1A_BASE64.toString(), remoteIndexMetadataManager.getPathHashAlgoSetting().toString());

        Settings newSettings = Settings.builder()
            .put("cluster.remote_store.index_metadata.path_hash_algo", RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1.toString())
            .build();
        clusterSettings.applySettings(newSettings);
        assertEquals(
            RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1.toString(),
            remoteIndexMetadataManager.getPathHashAlgoSetting().toString()
        );
    }

    private IndexMetadata getIndexMetadata(String name, @Nullable Boolean writeIndex, String... aliases) {
        IndexMetadata.Builder builder = IndexMetadata.builder(name)
            .settings(
                Settings.builder()
                    .put("index.version.created", Version.CURRENT.id)
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 1)
            );
        for (String alias : aliases) {
            builder.putAlias(AliasMetadata.builder(alias).writeIndex(writeIndex).build());
        }
        return builder.build();
    }
}
