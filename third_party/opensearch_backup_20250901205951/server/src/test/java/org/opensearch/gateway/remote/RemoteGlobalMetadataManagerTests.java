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
import org.density.cluster.ClusterModule;
import org.density.cluster.ClusterName;
import org.density.cluster.ClusterState;
import org.density.cluster.DiffableUtils;
import org.density.cluster.coordination.CoordinationMetadata;
import org.density.cluster.metadata.DiffableStringMap;
import org.density.cluster.metadata.IndexGraveyard;
import org.density.cluster.metadata.Metadata;
import org.density.cluster.metadata.TemplatesMetadata;
import org.density.common.blobstore.BlobPath;
import org.density.common.network.NetworkModule;
import org.density.common.settings.ClusterSettings;
import org.density.common.settings.Settings;
import org.density.common.util.TestCapturingListener;
import org.density.core.action.ActionListener;
import org.density.core.common.io.stream.NamedWriteableRegistry;
import org.density.core.compress.Compressor;
import org.density.core.compress.NoneCompressor;
import org.density.core.xcontent.NamedXContentRegistry;
import org.density.gateway.remote.model.RemoteCoordinationMetadata;
import org.density.gateway.remote.model.RemoteCustomMetadata;
import org.density.gateway.remote.model.RemoteGlobalMetadata;
import org.density.gateway.remote.model.RemoteHashesOfConsistentSettings;
import org.density.gateway.remote.model.RemotePersistentSettingsMetadata;
import org.density.gateway.remote.model.RemoteReadResult;
import org.density.gateway.remote.model.RemoteTemplatesMetadata;
import org.density.gateway.remote.model.RemoteTransientSettingsMetadata;
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
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.density.cluster.metadata.Metadata.isGlobalStateEquals;
import static org.density.common.blobstore.stream.write.WritePriority.URGENT;
import static org.density.gateway.remote.RemoteClusterStateTestUtils.CustomMetadata1;
import static org.density.gateway.remote.RemoteClusterStateTestUtils.CustomMetadata2;
import static org.density.gateway.remote.RemoteClusterStateTestUtils.CustomMetadata3;
import static org.density.gateway.remote.RemoteClusterStateTestUtils.CustomMetadata4;
import static org.density.gateway.remote.RemoteClusterStateTestUtils.CustomMetadata5;
import static org.density.gateway.remote.RemoteClusterStateUtils.CLUSTER_STATE_PATH_TOKEN;
import static org.density.gateway.remote.RemoteClusterStateUtils.CUSTOM_DELIMITER;
import static org.density.gateway.remote.RemoteClusterStateUtils.DELIMITER;
import static org.density.gateway.remote.RemoteClusterStateUtils.FORMAT_PARAMS;
import static org.density.gateway.remote.RemoteClusterStateUtils.GLOBAL_METADATA_CURRENT_CODEC_VERSION;
import static org.density.gateway.remote.RemoteClusterStateUtils.GLOBAL_METADATA_PATH_TOKEN;
import static org.density.gateway.remote.RemoteClusterStateUtils.PATH_DELIMITER;
import static org.density.gateway.remote.model.RemoteCoordinationMetadata.COORDINATION_METADATA;
import static org.density.gateway.remote.model.RemoteCoordinationMetadata.COORDINATION_METADATA_FORMAT;
import static org.density.gateway.remote.model.RemoteCoordinationMetadataTests.getCoordinationMetadata;
import static org.density.gateway.remote.model.RemoteCustomMetadata.CUSTOM_METADATA;
import static org.density.gateway.remote.model.RemoteCustomMetadataTests.getCustomMetadata;
import static org.density.gateway.remote.model.RemoteGlobalMetadata.GLOBAL_METADATA;
import static org.density.gateway.remote.model.RemoteGlobalMetadata.GLOBAL_METADATA_FORMAT;
import static org.density.gateway.remote.model.RemoteGlobalMetadataTests.getGlobalMetadata;
import static org.density.gateway.remote.model.RemoteHashesOfConsistentSettings.HASHES_OF_CONSISTENT_SETTINGS;
import static org.density.gateway.remote.model.RemoteHashesOfConsistentSettings.HASHES_OF_CONSISTENT_SETTINGS_FORMAT;
import static org.density.gateway.remote.model.RemoteHashesOfConsistentSettingsTests.getHashesOfConsistentSettings;
import static org.density.gateway.remote.model.RemotePersistentSettingsMetadata.SETTING_METADATA;
import static org.density.gateway.remote.model.RemotePersistentSettingsMetadataTests.getSettings;
import static org.density.gateway.remote.model.RemoteTemplatesMetadata.TEMPLATES_METADATA;
import static org.density.gateway.remote.model.RemoteTemplatesMetadata.TEMPLATES_METADATA_FORMAT;
import static org.density.gateway.remote.model.RemoteTemplatesMetadataTests.getTemplatesMetadata;
import static org.density.gateway.remote.model.RemoteTransientSettingsMetadata.TRANSIENT_SETTING_METADATA;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyIterable;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RemoteGlobalMetadataManagerTests extends DensityTestCase {
    private RemoteGlobalMetadataManager remoteGlobalMetadataManager;
    private ClusterSettings clusterSettings;
    private BlobStoreRepository blobStoreRepository;
    private BlobStoreTransferService blobStoreTransferService;
    private Compressor compressor;
    private NamedXContentRegistry xContentRegistry;
    private NamedWriteableRegistry namedWriteableRegistry;
    private final ThreadPool threadPool = new TestThreadPool(getClass().getName());
    private final long METADATA_VERSION = 7331L;
    private final String CLUSTER_NAME = "test-cluster";
    private final String CLUSTER_UUID = "test-cluster-uuid";

    @Before
    public void setup() {
        clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        blobStoreRepository = mock(BlobStoreRepository.class);
        blobStoreTransferService = mock(BlobStoreTransferService.class);
        compressor = new NoneCompressor();
        xContentRegistry = new NamedXContentRegistry(
            Stream.of(
                NetworkModule.getNamedXContents().stream(),
                IndicesModule.getNamedXContents().stream(),
                ClusterModule.getNamedXWriteables().stream()
            ).flatMap(Function.identity()).collect(toList())
        );
        namedWriteableRegistry = writableRegistry();
        BlobPath blobPath = new BlobPath();
        when(blobStoreRepository.getCompressor()).thenReturn(compressor);
        when(blobStoreRepository.getNamedXContentRegistry()).thenReturn(xContentRegistry);
        when(blobStoreRepository.basePath()).thenReturn(blobPath);
        remoteGlobalMetadataManager = new RemoteGlobalMetadataManager(
            clusterSettings,
            CLUSTER_NAME,
            blobStoreRepository,
            blobStoreTransferService,
            writableRegistry(),
            threadPool
        );
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdown();
    }

    public void testGlobalMetadataUploadWaitTimeSetting() {
        // verify default value
        assertEquals(
            RemoteGlobalMetadataManager.GLOBAL_METADATA_UPLOAD_TIMEOUT_DEFAULT,
            remoteGlobalMetadataManager.getGlobalMetadataUploadTimeout()
        );

        // verify update global metadata upload timeout
        int globalMetadataUploadTimeout = randomIntBetween(1, 10);
        Settings newSettings = Settings.builder()
            .put("cluster.remote_store.state.global_metadata.upload_timeout", globalMetadataUploadTimeout + "s")
            .build();
        clusterSettings.applySettings(newSettings);
        assertEquals(globalMetadataUploadTimeout, remoteGlobalMetadataManager.getGlobalMetadataUploadTimeout().seconds());
    }

    public void testGetAsyncReadRunnable_CoordinationMetadata() throws Exception {
        CoordinationMetadata coordinationMetadata = getCoordinationMetadata();
        String fileName = randomAlphaOfLength(10);
        RemoteCoordinationMetadata coordinationMetadataForDownload = new RemoteCoordinationMetadata(
            fileName,
            CLUSTER_UUID,
            compressor,
            xContentRegistry
        );
        when(blobStoreTransferService.downloadBlob(anyIterable(), anyString())).thenReturn(
            COORDINATION_METADATA_FORMAT.serialize(coordinationMetadata, fileName, compressor, FORMAT_PARAMS).streamInput()
        );
        TestCapturingListener<RemoteReadResult> listener = new TestCapturingListener<>();
        CountDownLatch latch = new CountDownLatch(1);

        remoteGlobalMetadataManager.readAsync(
            COORDINATION_METADATA,
            coordinationMetadataForDownload,
            new LatchedActionListener<>(listener, latch)
        );
        latch.await();
        assertNull(listener.getFailure());
        assertNotNull(listener.getResult());
        assertEquals(coordinationMetadata, listener.getResult().getObj());
        assertEquals(COORDINATION_METADATA, listener.getResult().getComponent());
        assertEquals(COORDINATION_METADATA, listener.getResult().getComponentName());
    }

    public void testGetAsyncWriteRunnable_CoordinationMetadata() throws Exception {
        CoordinationMetadata coordinationMetadata = getCoordinationMetadata();
        RemoteCoordinationMetadata remoteCoordinationMetadata = new RemoteCoordinationMetadata(
            coordinationMetadata,
            METADATA_VERSION,
            CLUSTER_UUID,
            compressor,
            xContentRegistry
        );
        doAnswer(invocationOnMock -> {
            invocationOnMock.getArgument(4, ActionListener.class).onResponse(null);
            return null;
        }).when(blobStoreTransferService)
            .uploadBlob(any(InputStream.class), anyIterable(), anyString(), eq(URGENT), any(ActionListener.class));
        TestCapturingListener<ClusterMetadataManifest.UploadedMetadata> listener = new TestCapturingListener<>();
        CountDownLatch latch = new CountDownLatch(1);

        remoteGlobalMetadataManager.writeAsync(
            COORDINATION_METADATA,
            remoteCoordinationMetadata,
            new LatchedActionListener<>(listener, latch)
        );
        latch.await();
        assertNull(listener.getFailure());
        assertNotNull(listener.getResult());
        ClusterMetadataManifest.UploadedMetadata uploadedMetadata = listener.getResult();
        assertEquals(COORDINATION_METADATA, uploadedMetadata.getComponent());
        String uploadedFileName = uploadedMetadata.getUploadedFilename();
        String[] pathTokens = uploadedFileName.split(PATH_DELIMITER);
        assertEquals(5, pathTokens.length);
        assertEquals(RemoteClusterStateUtils.encodeString(CLUSTER_NAME), pathTokens[0]);
        assertEquals(CLUSTER_STATE_PATH_TOKEN, pathTokens[1]);
        assertEquals(CLUSTER_UUID, pathTokens[2]);
        assertEquals(GLOBAL_METADATA_PATH_TOKEN, pathTokens[3]);
        String[] splitFileName = pathTokens[4].split(DELIMITER);
        assertEquals(4, splitFileName.length);
        assertEquals(COORDINATION_METADATA, splitFileName[0]);
        assertEquals(RemoteStoreUtils.invertLong(METADATA_VERSION), splitFileName[1]);
        assertEquals(GLOBAL_METADATA_CURRENT_CODEC_VERSION, Integer.parseInt(splitFileName[3]));
    }

    public void testGetAsyncReadRunnable_PersistentSettings() throws Exception {
        Settings settingsMetadata = getSettings();
        String fileName = randomAlphaOfLength(10);
        RemotePersistentSettingsMetadata persistentSettings = new RemotePersistentSettingsMetadata(
            fileName,
            CLUSTER_UUID,
            compressor,
            xContentRegistry
        );
        when(blobStoreTransferService.downloadBlob(anyIterable(), anyString())).thenReturn(
            RemotePersistentSettingsMetadata.SETTINGS_METADATA_FORMAT.serialize(settingsMetadata, fileName, compressor, FORMAT_PARAMS)
                .streamInput()
        );
        TestCapturingListener<RemoteReadResult> listener = new TestCapturingListener<>();
        CountDownLatch latch = new CountDownLatch(1);

        remoteGlobalMetadataManager.readAsync(SETTING_METADATA, persistentSettings, new LatchedActionListener<>(listener, latch));
        latch.await();
        assertNull(listener.getFailure());
        assertNotNull(listener.getResult());
        assertEquals(settingsMetadata, listener.getResult().getObj());
        assertEquals(SETTING_METADATA, listener.getResult().getComponent());
        assertEquals(SETTING_METADATA, listener.getResult().getComponentName());
    }

    public void testGetAsyncWriteRunnable_PersistentSettings() throws Exception {
        Settings settingsMetadata = getSettings();
        RemotePersistentSettingsMetadata persistentSettings = new RemotePersistentSettingsMetadata(
            settingsMetadata,
            METADATA_VERSION,
            CLUSTER_UUID,
            compressor,
            xContentRegistry
        );
        doAnswer(invocationOnMock -> {
            invocationOnMock.getArgument(4, ActionListener.class).onResponse(null);
            return null;
        }).when(blobStoreTransferService)
            .uploadBlob(any(InputStream.class), anyIterable(), anyString(), eq(URGENT), any(ActionListener.class));
        TestCapturingListener<ClusterMetadataManifest.UploadedMetadata> listener = new TestCapturingListener<>();
        CountDownLatch latch = new CountDownLatch(1);
        remoteGlobalMetadataManager.writeAsync(SETTING_METADATA, persistentSettings, new LatchedActionListener<>(listener, latch));

        latch.await();
        assertNull(listener.getFailure());
        assertNotNull(listener.getResult());
        ClusterMetadataManifest.UploadedMetadata uploadedMetadata = listener.getResult();
        assertEquals(SETTING_METADATA, uploadedMetadata.getComponent());
        String uploadedFileName = uploadedMetadata.getUploadedFilename();
        String[] pathTokens = uploadedFileName.split(PATH_DELIMITER);
        assertEquals(5, pathTokens.length);
        assertEquals(RemoteClusterStateUtils.encodeString(CLUSTER_NAME), pathTokens[0]);
        assertEquals(CLUSTER_STATE_PATH_TOKEN, pathTokens[1]);
        assertEquals(CLUSTER_UUID, pathTokens[2]);
        assertEquals(GLOBAL_METADATA_PATH_TOKEN, pathTokens[3]);
        String[] splitFileName = pathTokens[4].split(DELIMITER);
        assertEquals(4, splitFileName.length);
        assertEquals(SETTING_METADATA, splitFileName[0]);
        assertEquals(RemoteStoreUtils.invertLong(METADATA_VERSION), splitFileName[1]);
        assertEquals(GLOBAL_METADATA_CURRENT_CODEC_VERSION, Integer.parseInt(splitFileName[3]));
    }

    public void testGetAsyncReadRunnable_TransientSettings() throws Exception {
        Settings settingsMetadata = getSettings();
        String fileName = randomAlphaOfLength(10);
        RemoteTransientSettingsMetadata transientSettings = new RemoteTransientSettingsMetadata(
            fileName,
            CLUSTER_UUID,
            compressor,
            xContentRegistry
        );
        when(blobStoreTransferService.downloadBlob(anyIterable(), anyString())).thenReturn(
            RemoteTransientSettingsMetadata.SETTINGS_METADATA_FORMAT.serialize(settingsMetadata, fileName, compressor, FORMAT_PARAMS)
                .streamInput()
        );
        TestCapturingListener<RemoteReadResult> listener = new TestCapturingListener<>();
        CountDownLatch latch = new CountDownLatch(1);

        remoteGlobalMetadataManager.readAsync(TRANSIENT_SETTING_METADATA, transientSettings, new LatchedActionListener<>(listener, latch));
        latch.await();
        assertNull(listener.getFailure());
        assertNotNull(listener.getResult());
        assertEquals(settingsMetadata, listener.getResult().getObj());
        assertEquals(TRANSIENT_SETTING_METADATA, listener.getResult().getComponent());
        assertEquals(TRANSIENT_SETTING_METADATA, listener.getResult().getComponentName());
    }

    public void testGetAsyncWriteRunnable_TransientSettings() throws Exception {
        Settings settingsMetadata = getSettings();
        RemoteTransientSettingsMetadata transientSettings = new RemoteTransientSettingsMetadata(
            settingsMetadata,
            METADATA_VERSION,
            CLUSTER_UUID,
            compressor,
            xContentRegistry
        );
        doAnswer(invocationOnMock -> {
            invocationOnMock.getArgument(4, ActionListener.class).onResponse(null);
            return null;
        }).when(blobStoreTransferService)
            .uploadBlob(any(InputStream.class), anyIterable(), anyString(), eq(URGENT), any(ActionListener.class));
        TestCapturingListener<ClusterMetadataManifest.UploadedMetadata> listener = new TestCapturingListener<>();
        CountDownLatch latch = new CountDownLatch(1);
        remoteGlobalMetadataManager.writeAsync(TRANSIENT_SETTING_METADATA, transientSettings, new LatchedActionListener<>(listener, latch));
        latch.await();
        assertNull(listener.getFailure());
        assertNotNull(listener.getResult());
        ClusterMetadataManifest.UploadedMetadata uploadedMetadata = listener.getResult();
        assertEquals(TRANSIENT_SETTING_METADATA, uploadedMetadata.getComponent());
        String uploadedFileName = uploadedMetadata.getUploadedFilename();
        String[] pathTokens = uploadedFileName.split(PATH_DELIMITER);
        assertEquals(5, pathTokens.length);
        assertEquals(RemoteClusterStateUtils.encodeString(CLUSTER_NAME), pathTokens[0]);
        assertEquals(CLUSTER_STATE_PATH_TOKEN, pathTokens[1]);
        assertEquals(CLUSTER_UUID, pathTokens[2]);
        assertEquals(GLOBAL_METADATA_PATH_TOKEN, pathTokens[3]);
        String[] splitFileName = pathTokens[4].split(DELIMITER);
        assertEquals(4, splitFileName.length);
        assertEquals(TRANSIENT_SETTING_METADATA, splitFileName[0]);
        assertEquals(RemoteStoreUtils.invertLong(METADATA_VERSION), splitFileName[1]);
        assertEquals(GLOBAL_METADATA_CURRENT_CODEC_VERSION, Integer.parseInt(splitFileName[3]));
    }

    public void testGetAsyncReadRunnable_HashesOfConsistentSettings() throws Exception {
        DiffableStringMap hashesOfConsistentSettings = getHashesOfConsistentSettings();
        String fileName = randomAlphaOfLength(10);
        RemoteHashesOfConsistentSettings hashesOfConsistentSettingsForDownload = new RemoteHashesOfConsistentSettings(
            fileName,
            CLUSTER_UUID,
            compressor
        );
        when(blobStoreTransferService.downloadBlob(anyIterable(), anyString())).thenReturn(
            HASHES_OF_CONSISTENT_SETTINGS_FORMAT.serialize(hashesOfConsistentSettings, fileName, compressor).streamInput()
        );
        TestCapturingListener<RemoteReadResult> listener = new TestCapturingListener<>();
        CountDownLatch latch = new CountDownLatch(1);

        remoteGlobalMetadataManager.readAsync(
            HASHES_OF_CONSISTENT_SETTINGS,
            hashesOfConsistentSettingsForDownload,
            new LatchedActionListener<>(listener, latch)
        );
        latch.await();
        assertNull(listener.getFailure());
        assertNotNull(listener.getResult());
        assertEquals(hashesOfConsistentSettings, listener.getResult().getObj());
        assertEquals(HASHES_OF_CONSISTENT_SETTINGS, listener.getResult().getComponent());
        assertEquals(HASHES_OF_CONSISTENT_SETTINGS, listener.getResult().getComponentName());
    }

    public void testGetAsyncWriteRunnable_HashesOfConsistentSettings() throws Exception {
        DiffableStringMap hashesOfConsistentSettings = getHashesOfConsistentSettings();
        RemoteHashesOfConsistentSettings hashesOfConsistentSettingsForUpload = new RemoteHashesOfConsistentSettings(
            hashesOfConsistentSettings,
            METADATA_VERSION,
            CLUSTER_UUID,
            compressor
        );
        doAnswer(invocationOnMock -> {
            invocationOnMock.getArgument(4, ActionListener.class).onResponse(null);
            return null;
        }).when(blobStoreTransferService)
            .uploadBlob(any(InputStream.class), anyIterable(), anyString(), eq(URGENT), any(ActionListener.class));
        TestCapturingListener<ClusterMetadataManifest.UploadedMetadata> listener = new TestCapturingListener<>();
        CountDownLatch latch = new CountDownLatch(1);
        remoteGlobalMetadataManager.writeAsync(
            HASHES_OF_CONSISTENT_SETTINGS,
            hashesOfConsistentSettingsForUpload,
            new LatchedActionListener<>(listener, latch)
        );
        latch.await();
        assertNull(listener.getFailure());
        assertNotNull(listener.getResult());
        ClusterMetadataManifest.UploadedMetadata uploadedMetadata = listener.getResult();
        assertEquals(HASHES_OF_CONSISTENT_SETTINGS, uploadedMetadata.getComponent());
        String uploadedFileName = uploadedMetadata.getUploadedFilename();
        String[] pathTokens = uploadedFileName.split(PATH_DELIMITER);
        assertEquals(5, pathTokens.length);
        assertEquals(RemoteClusterStateUtils.encodeString(CLUSTER_NAME), pathTokens[0]);
        assertEquals(CLUSTER_STATE_PATH_TOKEN, pathTokens[1]);
        assertEquals(CLUSTER_UUID, pathTokens[2]);
        assertEquals(GLOBAL_METADATA_PATH_TOKEN, pathTokens[3]);
        String[] splitFileName = pathTokens[4].split(DELIMITER);
        assertEquals(4, splitFileName.length);
        assertEquals(HASHES_OF_CONSISTENT_SETTINGS, splitFileName[0]);
        assertEquals(RemoteStoreUtils.invertLong(METADATA_VERSION), splitFileName[1]);
        assertEquals(GLOBAL_METADATA_CURRENT_CODEC_VERSION, Integer.parseInt(splitFileName[3]));
    }

    public void testGetAsyncReadRunnable_TemplatesMetadata() throws Exception {
        TemplatesMetadata templatesMetadata = getTemplatesMetadata();
        String fileName = randomAlphaOfLength(10);
        RemoteTemplatesMetadata templatesMetadataForDownload = new RemoteTemplatesMetadata(
            fileName,
            CLUSTER_UUID,
            compressor,
            xContentRegistry
        );
        when(blobStoreTransferService.downloadBlob(anyIterable(), anyString())).thenReturn(
            TEMPLATES_METADATA_FORMAT.serialize(templatesMetadata, fileName, compressor, FORMAT_PARAMS).streamInput()
        );
        TestCapturingListener<RemoteReadResult> listener = new TestCapturingListener<>();
        CountDownLatch latch = new CountDownLatch(1);
        remoteGlobalMetadataManager.readAsync(
            TEMPLATES_METADATA,
            templatesMetadataForDownload,
            new LatchedActionListener<>(listener, latch)
        );
        latch.await();
        assertNull(listener.getFailure());
        assertNotNull(listener.getResult());
        assertEquals(templatesMetadata, listener.getResult().getObj());
        assertEquals(TEMPLATES_METADATA, listener.getResult().getComponent());
        assertEquals(TEMPLATES_METADATA, listener.getResult().getComponentName());
    }

    public void testGetAsyncWriteRunnable_TemplatesMetadata() throws Exception {
        TemplatesMetadata templatesMetadata = getTemplatesMetadata();
        RemoteTemplatesMetadata templateMetadataForUpload = new RemoteTemplatesMetadata(
            templatesMetadata,
            METADATA_VERSION,
            CLUSTER_UUID,
            compressor,
            xContentRegistry
        );
        doAnswer(invocationOnMock -> {
            invocationOnMock.getArgument(4, ActionListener.class).onResponse(null);
            return null;
        }).when(blobStoreTransferService)
            .uploadBlob(any(InputStream.class), anyIterable(), anyString(), eq(URGENT), any(ActionListener.class));
        TestCapturingListener<ClusterMetadataManifest.UploadedMetadata> listener = new TestCapturingListener<>();
        CountDownLatch latch = new CountDownLatch(1);
        remoteGlobalMetadataManager.writeAsync(TEMPLATES_METADATA, templateMetadataForUpload, new LatchedActionListener<>(listener, latch));
        latch.await();
        assertNull(listener.getFailure());
        assertNotNull(listener.getResult());
        ClusterMetadataManifest.UploadedMetadata uploadedMetadata = listener.getResult();
        assertEquals(TEMPLATES_METADATA, uploadedMetadata.getComponent());
        String uploadedFileName = uploadedMetadata.getUploadedFilename();
        String[] pathTokens = uploadedFileName.split(PATH_DELIMITER);
        assertEquals(5, pathTokens.length);
        assertEquals(RemoteClusterStateUtils.encodeString(CLUSTER_NAME), pathTokens[0]);
        assertEquals(CLUSTER_STATE_PATH_TOKEN, pathTokens[1]);
        assertEquals(CLUSTER_UUID, pathTokens[2]);
        assertEquals(GLOBAL_METADATA_PATH_TOKEN, pathTokens[3]);
        String[] splitFileName = pathTokens[4].split(DELIMITER);
        assertEquals(4, splitFileName.length);
        assertEquals(TEMPLATES_METADATA, splitFileName[0]);
        assertEquals(RemoteStoreUtils.invertLong(METADATA_VERSION), splitFileName[1]);
        assertEquals(GLOBAL_METADATA_CURRENT_CODEC_VERSION, Integer.parseInt(splitFileName[3]));
    }

    public void testGetAsyncReadRunnable_CustomMetadata() throws Exception {
        for (Version version : List.of(Version.CURRENT, Version.V_2_15_0, Version.V_2_13_0)) {
            verifyCustomMetadataReadForVersion(version);
        }
    }

    private void verifyCustomMetadataReadForVersion(Version version) throws Exception {
        Metadata.Custom customMetadata = getCustomMetadata();
        String fileName = randomAlphaOfLength(10);
        RemoteCustomMetadata customMetadataForDownload = new RemoteCustomMetadata(
            fileName,
            IndexGraveyard.TYPE,
            CLUSTER_UUID,
            compressor,
            namedWriteableRegistry,
            version
        );
        when(blobStoreTransferService.downloadBlob(anyIterable(), anyString())).thenReturn(
            customMetadataForDownload.customBlobStoreFormat.serialize(customMetadata, fileName, compressor).streamInput()
        );
        TestCapturingListener<RemoteReadResult> listener = new TestCapturingListener<>();
        CountDownLatch latch = new CountDownLatch(1);
        remoteGlobalMetadataManager.readAsync(IndexGraveyard.TYPE, customMetadataForDownload, new LatchedActionListener<>(listener, latch));
        latch.await();
        assertNull(listener.getFailure());
        assertNotNull(listener.getResult());
        assertEquals(customMetadata, listener.getResult().getObj());
        assertEquals(CUSTOM_METADATA, listener.getResult().getComponent());
        assertEquals(IndexGraveyard.TYPE, listener.getResult().getComponentName());
    }

    public void testGetAsyncWriteRunnable_CustomMetadata() throws Exception {
        Metadata.Custom customMetadata = getCustomMetadata();
        RemoteCustomMetadata customMetadataForUpload = new RemoteCustomMetadata(
            customMetadata,
            IndexGraveyard.TYPE,
            METADATA_VERSION,
            CLUSTER_UUID,
            compressor,
            namedWriteableRegistry
        );
        doAnswer(invocationOnMock -> {
            invocationOnMock.getArgument(4, ActionListener.class).onResponse(null);
            return null;
        }).when(blobStoreTransferService)
            .uploadBlob(any(InputStream.class), anyIterable(), anyString(), eq(URGENT), any(ActionListener.class));
        TestCapturingListener<ClusterMetadataManifest.UploadedMetadata> listener = new TestCapturingListener<>();
        CountDownLatch latch = new CountDownLatch(1);
        remoteGlobalMetadataManager.writeAsync(
            customMetadataForUpload.getType(),
            customMetadataForUpload,
            new LatchedActionListener<>(listener, latch)
        );
        latch.await();
        assertNull(listener.getFailure());
        assertNotNull(listener.getResult());
        ClusterMetadataManifest.UploadedMetadata uploadedMetadata = listener.getResult();
        assertEquals(String.join(CUSTOM_DELIMITER, CUSTOM_METADATA, IndexGraveyard.TYPE), uploadedMetadata.getComponent());
        String uploadedFileName = uploadedMetadata.getUploadedFilename();
        String[] pathTokens = uploadedFileName.split(PATH_DELIMITER);
        assertEquals(5, pathTokens.length);
        assertEquals(RemoteClusterStateUtils.encodeString(CLUSTER_NAME), pathTokens[0]);
        assertEquals(CLUSTER_STATE_PATH_TOKEN, pathTokens[1]);
        assertEquals(CLUSTER_UUID, pathTokens[2]);
        assertEquals(GLOBAL_METADATA_PATH_TOKEN, pathTokens[3]);
        String[] splitFileName = pathTokens[4].split(DELIMITER);
        assertEquals(4, splitFileName.length);
        assertEquals(String.join(CUSTOM_DELIMITER, CUSTOM_METADATA, IndexGraveyard.TYPE), splitFileName[0]);
        assertEquals(RemoteStoreUtils.invertLong(METADATA_VERSION), splitFileName[1]);
        assertEquals(GLOBAL_METADATA_CURRENT_CODEC_VERSION, Integer.parseInt(splitFileName[3]));
    }

    public void testGetAsyncReadRunnable_GlobalMetadata() throws Exception {
        Metadata metadata = getGlobalMetadata();
        String fileName = randomAlphaOfLength(10);
        RemoteGlobalMetadata globalMetadataForDownload = new RemoteGlobalMetadata(fileName, CLUSTER_UUID, compressor, xContentRegistry);
        when(blobStoreTransferService.downloadBlob(anyIterable(), anyString())).thenReturn(
            GLOBAL_METADATA_FORMAT.serialize(metadata, fileName, compressor, FORMAT_PARAMS).streamInput()
        );
        TestCapturingListener<RemoteReadResult> listener = new TestCapturingListener<>();
        CountDownLatch latch = new CountDownLatch(1);
        remoteGlobalMetadataManager.readAsync(GLOBAL_METADATA, globalMetadataForDownload, new LatchedActionListener<>(listener, latch));
        latch.await();
        assertNull(listener.getFailure());
        assertNotNull(listener.getResult());
        assertTrue(isGlobalStateEquals(metadata, (Metadata) listener.getResult().getObj()));
        assertEquals(GLOBAL_METADATA, listener.getResult().getComponent());
        assertEquals(GLOBAL_METADATA, listener.getResult().getComponentName());
    }

    public void testGetAsyncReadRunnable_IOException() throws Exception {
        String fileName = randomAlphaOfLength(10);
        RemoteCoordinationMetadata coordinationMetadataForDownload = new RemoteCoordinationMetadata(
            fileName,
            CLUSTER_UUID,
            compressor,
            xContentRegistry
        );
        IOException ioException = new IOException("mock test exception");
        when(blobStoreTransferService.downloadBlob(anyIterable(), anyString())).thenThrow(ioException);
        TestCapturingListener<RemoteReadResult> listener = new TestCapturingListener<>();
        CountDownLatch latch = new CountDownLatch(1);
        remoteGlobalMetadataManager.readAsync(
            COORDINATION_METADATA,
            coordinationMetadataForDownload,
            new LatchedActionListener<>(listener, latch)
        );
        latch.await();
        assertNull(listener.getResult());
        assertNotNull(listener.getFailure());
        assertEquals(ioException, listener.getFailure().getCause());
        assertTrue(listener.getFailure() instanceof RemoteStateTransferException);
    }

    public void testGetAsyncWriteRunnable_IOException() throws Exception {
        CoordinationMetadata coordinationMetadata = getCoordinationMetadata();
        RemoteCoordinationMetadata remoteCoordinationMetadata = new RemoteCoordinationMetadata(
            coordinationMetadata,
            METADATA_VERSION,
            CLUSTER_UUID,
            compressor,
            xContentRegistry
        );
        IOException ioException = new IOException("mock test exception");
        doAnswer(invocationOnMock -> {
            invocationOnMock.getArgument(4, ActionListener.class).onFailure(ioException);
            return null;
        }).when(blobStoreTransferService)
            .uploadBlob(any(InputStream.class), anyIterable(), anyString(), eq(URGENT), any(ActionListener.class));

        TestCapturingListener<ClusterMetadataManifest.UploadedMetadata> listener = new TestCapturingListener<>();
        CountDownLatch latch = new CountDownLatch(1);
        remoteGlobalMetadataManager.writeAsync(
            COORDINATION_METADATA,
            remoteCoordinationMetadata,
            new LatchedActionListener<>(listener, latch)
        );
        assertNull(listener.getResult());
        assertNotNull(listener.getFailure());
        assertTrue(listener.getFailure() instanceof RemoteStateTransferException);
        assertEquals(ioException, listener.getFailure().getCause());
    }

    public void testGetUpdatedCustoms() {
        Map<String, Metadata.Custom> previousCustoms = Map.of(
            CustomMetadata1.TYPE,
            new CustomMetadata1("data1"),
            CustomMetadata2.TYPE,
            new CustomMetadata2("data2"),
            CustomMetadata3.TYPE,
            new CustomMetadata3("data3")
        );
        ClusterState previousState = ClusterState.builder(new ClusterName("test-cluster"))
            .metadata(Metadata.builder().customs(previousCustoms))
            .build();

        Map<String, Metadata.Custom> currentCustoms = Map.of(
            CustomMetadata2.TYPE,
            new CustomMetadata2("data2"),
            CustomMetadata3.TYPE,
            new CustomMetadata3("data3-changed"),
            CustomMetadata4.TYPE,
            new CustomMetadata4("data4"),
            CustomMetadata5.TYPE,
            new CustomMetadata5("data5")
        );
        ClusterState currentState = ClusterState.builder(new ClusterName("test-cluster"))
            .metadata(Metadata.builder().customs(currentCustoms))
            .build();

        DiffableUtils.MapDiff<String, Metadata.Custom, Map<String, Metadata.Custom>> customsDiff = remoteGlobalMetadataManager
            .getCustomsDiff(currentState, previousState, true, false);
        Map<String, Metadata.Custom> expectedUpserts = Map.of(
            CustomMetadata2.TYPE,
            new CustomMetadata2("data2"),
            CustomMetadata3.TYPE,
            new CustomMetadata3("data3-changed"),
            CustomMetadata4.TYPE,
            new CustomMetadata4("data4"),
            IndexGraveyard.TYPE,
            IndexGraveyard.builder().build()
        );
        assertThat(customsDiff.getUpserts(), is(expectedUpserts));
        assertThat(customsDiff.getDeletes(), is(List.of()));

        customsDiff = remoteGlobalMetadataManager.getCustomsDiff(currentState, previousState, false, false);
        expectedUpserts = Map.of(
            CustomMetadata3.TYPE,
            new CustomMetadata3("data3-changed"),
            CustomMetadata4.TYPE,
            new CustomMetadata4("data4")
        );
        assertThat(customsDiff.getUpserts(), is(expectedUpserts));
        assertThat(customsDiff.getDeletes(), is(List.of(CustomMetadata1.TYPE)));

        customsDiff = remoteGlobalMetadataManager.getCustomsDiff(currentState, previousState, true, true);
        expectedUpserts = Map.of(
            CustomMetadata2.TYPE,
            new CustomMetadata2("data2"),
            CustomMetadata3.TYPE,
            new CustomMetadata3("data3-changed"),
            CustomMetadata4.TYPE,
            new CustomMetadata4("data4"),
            CustomMetadata5.TYPE,
            new CustomMetadata5("data5"),
            IndexGraveyard.TYPE,
            IndexGraveyard.builder().build()
        );
        assertThat(customsDiff.getUpserts(), is(expectedUpserts));
        assertThat(customsDiff.getDeletes(), is(List.of()));

        customsDiff = remoteGlobalMetadataManager.getCustomsDiff(currentState, previousState, false, true);
        expectedUpserts = Map.of(
            CustomMetadata3.TYPE,
            new CustomMetadata3("data3-changed"),
            CustomMetadata4.TYPE,
            new CustomMetadata4("data4"),
            CustomMetadata5.TYPE,
            new CustomMetadata5("data5")
        );
        assertThat(customsDiff.getUpserts(), is(expectedUpserts));
        assertThat(customsDiff.getDeletes(), is(List.of(CustomMetadata1.TYPE)));
    }

}
