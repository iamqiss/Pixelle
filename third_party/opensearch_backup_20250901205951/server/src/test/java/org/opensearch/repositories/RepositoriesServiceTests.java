/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright Density Contributors. See
 * GitHub history for details.
 */

package org.density.repositories;

import org.apache.lucene.index.IndexCommit;
import org.density.Version;
import org.density.action.admin.cluster.crypto.CryptoSettings;
import org.density.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.density.cluster.AckedClusterStateUpdateTask;
import org.density.cluster.ClusterChangedEvent;
import org.density.cluster.ClusterName;
import org.density.cluster.ClusterState;
import org.density.cluster.ClusterStateUpdateTask;
import org.density.cluster.ack.ClusterStateUpdateResponse;
import org.density.cluster.metadata.CryptoMetadata;
import org.density.cluster.metadata.IndexMetadata;
import org.density.cluster.metadata.Metadata;
import org.density.cluster.metadata.RepositoriesMetadata;
import org.density.cluster.metadata.RepositoryMetadata;
import org.density.cluster.node.DiscoveryNode;
import org.density.cluster.node.DiscoveryNodes;
import org.density.cluster.service.ClusterApplierService;
import org.density.cluster.service.ClusterService;
import org.density.common.Priority;
import org.density.common.UUIDs;
import org.density.common.blobstore.BlobPath;
import org.density.common.blobstore.BlobStore;
import org.density.common.crypto.CryptoHandler;
import org.density.common.crypto.DecryptedRangedStreamProvider;
import org.density.common.crypto.EncryptedHeaderContentSupplier;
import org.density.common.crypto.MasterKeyProvider;
import org.density.common.io.InputStreamContainer;
import org.density.common.lifecycle.Lifecycle;
import org.density.common.lifecycle.LifecycleListener;
import org.density.common.settings.ClusterSettings;
import org.density.common.settings.Settings;
import org.density.core.action.ActionListener;
import org.density.core.common.Strings;
import org.density.core.index.shard.ShardId;
import org.density.core.xcontent.NamedXContentRegistry;
import org.density.index.mapper.MapperService;
import org.density.index.snapshots.IndexShardSnapshotStatus;
import org.density.index.snapshots.blobstore.RemoteStoreShardShallowCopySnapshot;
import org.density.index.store.Store;
import org.density.index.store.lockmanager.RemoteStoreLockManagerFactory;
import org.density.indices.recovery.RecoverySettings;
import org.density.indices.recovery.RecoveryState;
import org.density.plugins.CryptoPlugin;
import org.density.repositories.blobstore.MeteredBlobStoreRepository;
import org.density.snapshots.SnapshotId;
import org.density.snapshots.SnapshotInfo;
import org.density.telemetry.tracing.noop.NoopTracer;
import org.density.test.DensityTestCase;
import org.density.threadpool.ThreadPool;
import org.density.transport.Transport;
import org.density.transport.TransportService;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.density.repositories.blobstore.BlobStoreRepository.SYSTEM_REPOSITORY_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RepositoriesServiceTests extends DensityTestCase {

    private RepositoriesService repositoriesService;
    private final String kpTypeA = "kp-type-a";
    private final String kpTypeB = "kp-type-b";

    @Override
    public void setUp() throws Exception {
        super.setUp();
        ThreadPool threadPool = mock(ThreadPool.class);
        final ClusterApplierService clusterApplierService = mock(ClusterApplierService.class);
        when(clusterApplierService.threadPool()).thenReturn(threadPool);
        final ClusterService clusterService = mock(ClusterService.class);
        repositoriesService = createRepositoriesServiceWithMockedClusterService(clusterService);
    }

    private RepositoriesService createRepositoriesServiceWithMockedClusterService(ClusterService clusterService) {
        ThreadPool threadPool = mock(ThreadPool.class);
        final TransportService transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundAddress -> DiscoveryNode.createLocal(Settings.EMPTY, boundAddress.publishAddress(), UUIDs.randomBase64UUID()),
            null,
            Collections.emptySet(),
            NoopTracer.INSTANCE
        );
        final ClusterApplierService clusterApplierService = mock(ClusterApplierService.class);
        when(clusterApplierService.threadPool()).thenReturn(threadPool);
        when(clusterService.getClusterApplierService()).thenReturn(clusterApplierService);
        Map<String, Repository.Factory> typesRegistry = Map.of(
            TestRepository.TYPE,
            TestRepository::new,
            MeteredRepositoryTypeA.TYPE,
            metadata -> new MeteredRepositoryTypeA(metadata, clusterService),
            MeteredRepositoryTypeB.TYPE,
            metadata -> new MeteredRepositoryTypeB(metadata, clusterService)
        );

        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        when(nodes.getMinNodeVersion()).thenReturn(Version.V_2_9_0);
        ClusterState currentClusterState = mock(ClusterState.class);
        when(currentClusterState.getNodes()).thenReturn(nodes);
        when(clusterService.state()).thenReturn(currentClusterState);

        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        RepositoriesService repositoriesService = new RepositoriesService(
            Settings.EMPTY,
            clusterService,
            transportService,
            typesRegistry,
            typesRegistry,
            threadPool
        );

        repositoriesService.start();
        return repositoriesService;
    }

    public void testRegisterInternalRepository() {
        String repoName = "name";
        expectThrows(RepositoryMissingException.class, () -> repositoriesService.repository(repoName));
        repositoriesService.registerInternalRepository(repoName, TestRepository.TYPE);
        Repository repository = repositoriesService.repository(repoName);
        assertEquals(repoName, repository.getMetadata().name());
        assertEquals(TestRepository.TYPE, repository.getMetadata().type());
        assertEquals(Settings.EMPTY, repository.getMetadata().settings());
        assertTrue(((TestRepository) repository).isStarted);
    }

    public void testUnregisterInternalRepository() {
        String repoName = "name";
        expectThrows(RepositoryMissingException.class, () -> repositoriesService.repository(repoName));
        repositoriesService.registerInternalRepository(repoName, TestRepository.TYPE);
        Repository repository = repositoriesService.repository(repoName);
        assertFalse(((TestRepository) repository).isClosed);
        repositoriesService.unregisterInternalRepository(repoName);
        expectThrows(RepositoryMissingException.class, () -> repositoriesService.repository(repoName));
        assertTrue(((TestRepository) repository).isClosed);
    }

    public void testRegisterWillNotUpdateIfInternalRepositoryWithNameExists() {
        String repoName = "name";
        expectThrows(RepositoryMissingException.class, () -> repositoriesService.repository(repoName));
        repositoriesService.registerInternalRepository(repoName, TestRepository.TYPE);
        Repository repository = repositoriesService.repository(repoName);
        assertFalse(((TestRepository) repository).isClosed);
        repositoriesService.registerInternalRepository(repoName, TestRepository.TYPE);
        assertFalse(((TestRepository) repository).isClosed);
        Repository repository2 = repositoriesService.repository(repoName);
        assertSame(repository, repository2);
    }

    public void testRegisterRejectsInvalidRepositoryNames() {
        assertThrowsOnRegister("");
        assertThrowsOnRegister("contains#InvalidCharacter");
        for (char c : Strings.INVALID_FILENAME_CHARS) {
            assertThrowsOnRegister("contains" + c + "InvalidCharacters");
        }
    }

    public void testUpdateOrRegisterRejectsForSystemRepository() {
        String repoName = "name";
        PutRepositoryRequest request = new PutRepositoryRequest(repoName);
        request.settings(Settings.builder().put(SYSTEM_REPOSITORY_SETTING.getKey(), true).build());
        expectThrows(RepositoryException.class, () -> repositoriesService.registerOrUpdateRepository(request, null));
    }

    public void testRepositoriesStatsCanHaveTheSameNameAndDifferentTypeOverTime() {
        String repoName = "name";
        expectThrows(RepositoryMissingException.class, () -> repositoriesService.repository(repoName));

        ClusterState clusterStateWithRepoTypeA = createClusterStateWithRepo(repoName, MeteredRepositoryTypeA.TYPE);

        repositoriesService.applyClusterState(new ClusterChangedEvent("new repo", clusterStateWithRepoTypeA, emptyState()));
        assertThat(repositoriesService.repositoriesStats().size(), equalTo(1));

        repositoriesService.applyClusterState(new ClusterChangedEvent("new repo", emptyState(), clusterStateWithRepoTypeA));
        assertThat(repositoriesService.repositoriesStats().size(), equalTo(0));

        ClusterState clusterStateWithRepoTypeB = createClusterStateWithRepo(repoName, MeteredRepositoryTypeB.TYPE);
        repositoriesService.applyClusterState(new ClusterChangedEvent("new repo", clusterStateWithRepoTypeB, emptyState()));

        List<RepositoryStatsSnapshot> repositoriesStats = repositoriesService.repositoriesStats();
        assertThat(repositoriesStats.size(), equalTo(1));
        RepositoryStatsSnapshot repositoryStatsTypeA = repositoriesStats.get(0);
        assertThat(repositoryStatsTypeA.getRepositoryInfo().type, equalTo(MeteredRepositoryTypeB.TYPE));
        assertThat(repositoryStatsTypeA.getRepositoryStats(), equalTo(MeteredRepositoryTypeB.STATS));

    }

    public void testWithSameKeyProviderNames() {
        String keyProviderName = "kp-name";
        ClusterState clusterStateWithRepoTypeA = createClusterStateWithKeyProvider(
            "repoName",
            MeteredRepositoryTypeA.TYPE,
            keyProviderName,
            kpTypeA
        );

        repositoriesService.applyClusterState(new ClusterChangedEvent("new repo", clusterStateWithRepoTypeA, emptyState()));
        assertThat(repositoriesService.repositoriesStats().size(), equalTo(1));
        MeteredRepositoryTypeA repository = (MeteredRepositoryTypeA) repositoriesService.repository("repoName");
        assertNotNull(repository);
        assertNotNull(repository.cryptoHandler);
        assertEquals(kpTypeA, repository.cryptoHandler.kpType);

        ClusterState clusterStateWithRepoTypeB = createClusterStateWithKeyProvider(
            "repoName",
            MeteredRepositoryTypeB.TYPE,
            keyProviderName,
            kpTypeA
        );
        repositoriesService.applyClusterState(new ClusterChangedEvent("new repo", clusterStateWithRepoTypeB, emptyState()));
        assertThat(repositoriesService.repositoriesStats().size(), equalTo(1));
        MeteredRepositoryTypeB repositoryB = (MeteredRepositoryTypeB) repositoriesService.repository("repoName");
        assertNotNull(repositoryB);
        assertNotNull(repository.cryptoHandler);
        assertEquals(kpTypeA, repository.cryptoHandler.kpType);
    }

    public void testCryptoManagersUnchangedWithSameCryptoMetadata() {
        String keyProviderName = "kp-name";
        ClusterState clusterStateWithRepoTypeA = createClusterStateWithKeyProvider(
            "repoName",
            MeteredRepositoryTypeA.TYPE,
            keyProviderName,
            kpTypeA
        );
        repositoriesService.applyClusterState(new ClusterChangedEvent("new repo", clusterStateWithRepoTypeA, emptyState()));
        assertThat(repositoriesService.repositoriesStats().size(), equalTo(1));
        MeteredRepositoryTypeA repository = (MeteredRepositoryTypeA) repositoriesService.repository("repoName");
        assertNotNull(repository);
        assertNotNull(repository.cryptoHandler);
        assertEquals(kpTypeA, repository.cryptoHandler.kpType);

        repositoriesService.applyClusterState(new ClusterChangedEvent("new repo", clusterStateWithRepoTypeA, emptyState()));
        assertThat(repositoriesService.repositoriesStats().size(), equalTo(1));
        repository = (MeteredRepositoryTypeA) repositoriesService.repository("repoName");
        assertNotNull(repository);
        assertNotNull(repository.cryptoHandler);
        assertEquals(kpTypeA, repository.cryptoHandler.kpType);
    }

    public void testRepositoryUpdateWithDifferentCryptoMetadata() {
        String keyProviderName = "kp-name";

        ClusterState clusterStateWithRepoTypeA = createClusterStateWithKeyProvider(
            "repoName",
            MeteredRepositoryTypeA.TYPE,
            keyProviderName,
            kpTypeA
        );
        ClusterService clusterService = mock(ClusterService.class);

        PutRepositoryRequest request = new PutRepositoryRequest("repoName");
        request.type(MeteredRepositoryTypeA.TYPE);
        request.settings(Settings.EMPTY);

        doAnswer((invocation) -> {
            AckedClusterStateUpdateTask<ClusterStateUpdateResponse> task = (AckedClusterStateUpdateTask<
                ClusterStateUpdateResponse>) invocation.getArguments()[1];
            task.execute(clusterStateWithRepoTypeA);
            return null;
        }).when(clusterService).submitStateUpdateTask(any(), any());

        RepositoriesService repositoriesService = createRepositoriesServiceWithMockedClusterService(clusterService);
        repositoriesService.applyClusterState(new ClusterChangedEvent("new repo", clusterStateWithRepoTypeA, emptyState()));
        assertThat(repositoriesService.repositoriesStats().size(), equalTo(1));
        MeteredRepositoryTypeA repository = (MeteredRepositoryTypeA) repositoriesService.repository("repoName");
        assertNotNull(repository);
        assertNotNull(repository.cryptoHandler);
        assertEquals(kpTypeA, repository.cryptoHandler.kpType);

        expectThrows(IllegalArgumentException.class, () -> repositoriesService.registerOrUpdateRepository(request, null));

        CryptoSettings cryptoSettings = new CryptoSettings(keyProviderName);
        cryptoSettings.keyProviderType(kpTypeA);
        cryptoSettings.settings(Settings.builder().put("key-1", "val-1"));
        request.cryptoSettings(cryptoSettings);
        expectThrows(IllegalArgumentException.class, () -> repositoriesService.registerOrUpdateRepository(request, null));

        cryptoSettings.settings(Settings.builder());
        cryptoSettings.keyProviderName("random");
        expectThrows(IllegalArgumentException.class, () -> repositoriesService.registerOrUpdateRepository(request, null));

        cryptoSettings.keyProviderName(keyProviderName);

        assertEquals(kpTypeA, repository.cryptoHandler.kpType);
        repositoriesService.registerOrUpdateRepository(request, null);
    }

    public void testCryptoManagerClusterStateChanges() {

        ClusterService clusterService = mock(ClusterService.class);
        AtomicBoolean verified = new AtomicBoolean();
        List<RepositoryMetadata> repositoryMetadata = new ArrayList<>();

        String keyProviderName = "kp-name-1";
        String repoName = "repoName";
        String keyProviderType = kpTypeA;
        Settings.Builder settings = Settings.builder();
        PutRepositoryRequest request = createPutRepositoryEncryptedRequest(
            repoName,
            MeteredRepositoryTypeA.TYPE,
            keyProviderName,
            settings,
            keyProviderType
        );
        verified.set(false);
        RepositoriesService repositoriesService = createRepositoriesServiceAndMockCryptoClusterState(
            clusterService,
            repoName,
            keyProviderName,
            keyProviderType,
            settings.build(),
            verified,
            repositoryMetadata
        );
        repositoriesService.registerOrUpdateRepository(request, null);
        MeteredRepositoryTypeA repository = (MeteredRepositoryTypeA) repositoriesService.repository(repoName);
        assertNotNull(repository.cryptoHandler);
        assertEquals(kpTypeA, repository.cryptoHandler.kpType);
        assertTrue(verified.get());

        // No change
        keyProviderType = kpTypeA;
        settings = Settings.builder();
        request = createPutRepositoryEncryptedRequest(repoName, MeteredRepositoryTypeA.TYPE, keyProviderName, settings, keyProviderType);
        verified.set(false);
        repositoriesService = createRepositoriesServiceAndMockCryptoClusterState(
            clusterService,
            repoName,
            keyProviderName,
            keyProviderType,
            settings.build(),
            verified,
            repositoryMetadata
        );
        repositoriesService.registerOrUpdateRepository(request, null);

        repository = (MeteredRepositoryTypeA) repositoriesService.repository(repoName);
        assertNotNull(repository.cryptoHandler);
        assertEquals(kpTypeA, repository.cryptoHandler.kpType);
        assertTrue(verified.get());

        // Same crypto client in new repo
        repoName = "repoName-2";
        keyProviderType = kpTypeA;
        settings = Settings.builder();
        request = createPutRepositoryEncryptedRequest(repoName, MeteredRepositoryTypeA.TYPE, keyProviderName, settings, keyProviderType);
        verified.set(false);
        repositoriesService = createRepositoriesServiceAndMockCryptoClusterState(
            clusterService,
            repoName,
            keyProviderName,
            keyProviderType,
            settings.build(),
            verified,
            repositoryMetadata
        );
        repositoriesService.registerOrUpdateRepository(request, null);
        repository = (MeteredRepositoryTypeA) repositoriesService.repository(repoName);
        assertNotNull(repository.cryptoHandler);
        assertEquals(kpTypeA, repository.cryptoHandler.kpType);
        assertTrue(verified.get());

        // Different crypto client in new repo
        repoName = "repoName-3";
        keyProviderType = kpTypeB;
        settings = Settings.builder();
        request = createPutRepositoryEncryptedRequest(repoName, MeteredRepositoryTypeA.TYPE, keyProviderName, settings, keyProviderType);
        verified.set(false);
        repositoriesService = createRepositoriesServiceAndMockCryptoClusterState(
            clusterService,
            repoName,
            keyProviderName,
            keyProviderType,
            settings.build(),
            verified,
            repositoryMetadata
        );
        repositoriesService.registerOrUpdateRepository(request, null);
        repository = (MeteredRepositoryTypeA) repositoriesService.repository(repoName);
        assertNotNull(repository.cryptoHandler);
        assertEquals(kpTypeB, repository.cryptoHandler.kpType);
        assertTrue(verified.get());

    }

    private RepositoriesService createRepositoriesServiceAndMockCryptoClusterState(
        ClusterService clusterService,
        String repoName,
        String keyProviderName,
        String keyProviderType,
        Settings settings,
        AtomicBoolean verified,
        List<RepositoryMetadata> repositoryMetadataList
    ) {

        ClusterState.Builder state = ClusterState.builder(new ClusterName("test"));
        CryptoMetadata newCryptoMetadata = new CryptoMetadata(keyProviderName, keyProviderType, Settings.EMPTY);
        Metadata.Builder mdBuilder = Metadata.builder();

        RepositoryMetadata newRepositoryMetadata = new RepositoryMetadata(
            repoName,
            MeteredRepositoryTypeA.TYPE,
            Settings.EMPTY,
            newCryptoMetadata
        );
        if (!repositoryMetadataList.contains(newRepositoryMetadata)) {
            repositoryMetadataList.add(newRepositoryMetadata);
        }
        RepositoriesMetadata newRepositoriesMetadata = new RepositoriesMetadata(repositoryMetadataList);
        mdBuilder.putCustom(RepositoriesMetadata.TYPE, newRepositoriesMetadata);
        state.metadata(mdBuilder);
        ClusterState clusterStateWithRepoTypeA = state.build();

        RepositoriesService repositoriesService = createRepositoriesServiceWithMockedClusterService(clusterService);

        doAnswer((invocation) -> {
            AckedClusterStateUpdateTask<ClusterStateUpdateResponse> task = (AckedClusterStateUpdateTask<
                ClusterStateUpdateResponse>) invocation.getArguments()[1];
            ClusterState clusterState = task.execute(clusterStateWithRepoTypeA);
            RepositoriesMetadata repositories = clusterState.metadata().custom(RepositoriesMetadata.TYPE);
            RepositoryMetadata repositoryMetadata = repositories.repositories().get(repositoryMetadataList.size() - 1);
            CryptoMetadata cryptoMetadata = repositoryMetadata.cryptoMetadata();
            assertNotNull(cryptoMetadata);
            assertEquals(keyProviderName, cryptoMetadata.keyProviderName());
            assertEquals(keyProviderType, cryptoMetadata.keyProviderType());
            assertEquals(cryptoMetadata.settings(), settings);
            verified.set(true);
            repositoriesService.applyClusterState(new ClusterChangedEvent("new repo", clusterStateWithRepoTypeA, emptyState()));
            return null;
        }).when(clusterService).submitStateUpdateTask(any(), any());

        return repositoriesService;
    }

    private ClusterState createClusterStateWithRepo(String repoName, String repoType) {
        ClusterState.Builder state = ClusterState.builder(new ClusterName("test"));
        Metadata.Builder mdBuilder = Metadata.builder();
        mdBuilder.putCustom(
            RepositoriesMetadata.TYPE,
            new RepositoriesMetadata(Collections.singletonList(new RepositoryMetadata(repoName, repoType, Settings.EMPTY)))
        );
        state.metadata(mdBuilder);

        return state.build();
    }

    private ClusterState createClusterStateWithKeyProvider(
        String repoName,
        String repoType,
        String keyProviderName,
        String keyProviderType
    ) {
        ClusterState.Builder state = ClusterState.builder(new ClusterName("test"));
        Metadata.Builder mdBuilder = Metadata.builder();
        CryptoMetadata cryptoMetadata = new CryptoMetadata(keyProviderName, keyProviderType, Settings.EMPTY);
        mdBuilder.putCustom(
            RepositoriesMetadata.TYPE,
            new RepositoriesMetadata(Collections.singletonList(new RepositoryMetadata(repoName, repoType, Settings.EMPTY, cryptoMetadata)))
        );
        state.metadata(mdBuilder);

        return state.build();
    }

    private PutRepositoryRequest createPutRepositoryEncryptedRequest(
        String repoName,
        String repoType,
        String keyProviderName,
        Settings.Builder settings,
        String keyProviderType
    ) {
        PutRepositoryRequest repositoryRequest = new PutRepositoryRequest(repoName);
        repositoryRequest.type(repoType);
        repositoryRequest.settings(Settings.EMPTY);
        CryptoSettings cryptoSettings = new CryptoSettings(keyProviderName);
        cryptoSettings.keyProviderName(keyProviderName);
        cryptoSettings.keyProviderType(keyProviderType);
        cryptoSettings.settings(settings);
        repositoryRequest.cryptoSettings(cryptoSettings);

        return repositoryRequest;
    }

    private ClusterState emptyState() {
        return ClusterState.builder(new ClusterName("test")).build();
    }

    private void assertThrowsOnRegister(String repoName) {
        PutRepositoryRequest request = new PutRepositoryRequest(repoName);
        expectThrows(RepositoryException.class, () -> repositoriesService.registerOrUpdateRepository(request, null));
    }

    private static class TestCryptoProvider implements CryptoHandler<Object, Object> {
        final String kpName;
        final String kpType;

        public TestCryptoProvider(String kpName, String kpType) {
            this.kpName = kpName;
            this.kpType = kpType;
        }

        @Override
        public Object initEncryptionMetadata() {
            return new Object();
        }

        @Override
        public long adjustContentSizeForPartialEncryption(Object cryptoContextObj, long contentSize) {
            return 0;
        }

        @Override
        public long estimateEncryptedLengthOfEntireContent(Object cryptoContextObj, long contentLength) {
            return 0;
        }

        @Override
        public InputStreamContainer createEncryptingStream(Object encryptionMetadata, InputStreamContainer streamContainer) {
            return null;
        }

        @Override
        public InputStreamContainer createEncryptingStreamOfPart(
            Object cryptoContextObj,
            InputStreamContainer stream,
            int totalStreams,
            int streamIdx
        ) {
            return null;
        }

        @Override
        public InputStream createDecryptingStream(InputStream encryptingStream) {
            return null;
        }

        @Override
        public Object loadEncryptionMetadata(EncryptedHeaderContentSupplier encryptedHeaderContentSupplier) throws IOException {
            return null;
        }

        @Override
        public DecryptedRangedStreamProvider createDecryptingStreamOfRange(
            Object cryptoContext,
            long startPosOfRawContent,
            long endPosOfRawContent
        ) {
            return null;
        }

        @Override
        public long estimateDecryptedLength(Object cryptoContext, long contentLength) {
            return 0;
        }

        @Override
        public void close() throws IOException {

        }
    }

    private static abstract class TestCryptoHandler implements CryptoPlugin<Object, Object> {
        private final Settings settings;

        public TestCryptoHandler(Settings settings) {
            this.settings = settings;
        }

        public CryptoHandler<Object, Object> getOrCreateCryptoHandler(
            MasterKeyProvider keyProvider,
            String keyProviderName,
            String keyProviderType,
            Runnable onClose
        ) {
            return new TestCryptoProvider(keyProviderName, keyProviderType);
        }
    }

    private static class TestRepository implements Repository {

        private static final String TYPE = "internal";
        private boolean isClosed;
        private boolean isStarted;

        private final RepositoryMetadata metadata;

        private TestRepository(RepositoryMetadata metadata) {
            this.metadata = metadata;
        }

        @Override
        public RepositoryMetadata getMetadata() {
            return metadata;
        }

        @Override
        public SnapshotInfo getSnapshotInfo(SnapshotId snapshotId) {
            return null;
        }

        @Override
        public Metadata getSnapshotGlobalMetadata(SnapshotId snapshotId) {
            return null;
        }

        @Override
        public IndexMetadata getSnapshotIndexMetaData(RepositoryData repositoryData, SnapshotId snapshotId, IndexId index) {
            return null;
        }

        @Override
        public void getRepositoryData(ActionListener<RepositoryData> listener) {
            listener.onResponse(null);
        }

        @Override
        public void finalizeSnapshot(
            ShardGenerations shardGenerations,
            long repositoryStateId,
            Metadata clusterMetadata,
            SnapshotInfo snapshotInfo,
            Version repositoryMetaVersion,
            Function<ClusterState, ClusterState> stateTransformer,
            Priority repositoryUpdatePriority,
            ActionListener<RepositoryData> listener
        ) {
            listener.onResponse(null);
        }

        @Override
        public void finalizeSnapshot(
            ShardGenerations shardGenerations,
            long repositoryStateId,
            Metadata clusterMetadata,
            SnapshotInfo snapshotInfo,
            Version repositoryMetaVersion,
            Function<ClusterState, ClusterState> stateTransformer,
            ActionListener<RepositoryData> listener
        ) {
            listener.onResponse(null);
        }

        @Override
        public void deleteSnapshots(
            Collection<SnapshotId> snapshotIds,
            long repositoryStateId,
            Version repositoryMetaVersion,
            ActionListener<RepositoryData> listener
        ) {
            listener.onResponse(null);
        }

        @Override
        public long getSnapshotThrottleTimeInNanos() {
            return 0;
        }

        @Override
        public long getRestoreThrottleTimeInNanos() {
            return 0;
        }

        @Override
        public long getRemoteUploadThrottleTimeInNanos() {
            return 0;
        }

        @Override
        public long getRemoteDownloadThrottleTimeInNanos() {
            return 0;
        }

        @Override
        public long getLowPriorityRemoteDownloadThrottleTimeInNanos() {
            return 0;
        }

        @Override
        public String startVerification() {
            return null;
        }

        @Override
        public void endVerification(String verificationToken) {

        }

        @Override
        public void verify(String verificationToken, DiscoveryNode localNode) {

        }

        @Override
        public boolean isReadOnly() {
            return false;
        }

        @Override
        public boolean isSystemRepository() {
            return false;
        }

        @Override
        public void snapshotShard(
            Store store,
            MapperService mapperService,
            SnapshotId snapshotId,
            IndexId indexId,
            IndexCommit snapshotIndexCommit,
            String shardStateIdentifier,
            IndexShardSnapshotStatus snapshotStatus,
            Version repositoryMetaVersion,
            Map<String, Object> userMetadata,
            ActionListener<String> listener
        ) {

        }

        @Override
        public void snapshotRemoteStoreIndexShard(
            Store store,
            SnapshotId snapshotId,
            IndexId indexId,
            IndexCommit snapshotIndexCommit,
            String shardStateIdentifier,
            IndexShardSnapshotStatus snapshotStatus,
            long primaryTerm,
            long commitGeneration,
            long startTime,
            Map<String, Long> indexFilesToFileLengthMap,
            ActionListener<String> listener
        ) {

        }

        @Override
        public void restoreShard(
            Store store,
            SnapshotId snapshotId,
            IndexId indexId,
            ShardId snapshotShardId,
            RecoveryState recoveryState,
            ActionListener<Void> listener
        ) {

        }

        @Override
        public RemoteStoreShardShallowCopySnapshot getRemoteStoreShallowCopyShardMetadata(
            SnapshotId snapshotId,
            IndexId indexId,
            ShardId snapshotShardId
        ) {
            return null;
        }

        @Override
        public IndexShardSnapshotStatus getShardSnapshotStatus(SnapshotId snapshotId, IndexId indexId, ShardId shardId) {
            return null;
        }

        @Override
        public void updateState(final ClusterState state) {}

        @Override
        public void executeConsistentStateUpdate(
            Function<RepositoryData, ClusterStateUpdateTask> createUpdateTask,
            String source,
            Consumer<Exception> onFailure
        ) {}

        @Override
        public void cloneShardSnapshot(
            SnapshotId source,
            SnapshotId target,
            RepositoryShardId shardId,
            String shardGeneration,
            ActionListener<String> listener
        ) {

        }

        @Override
        public void cloneRemoteStoreIndexShardSnapshot(
            SnapshotId source,
            SnapshotId target,
            RepositoryShardId shardId,
            String shardGeneration,
            RemoteStoreLockManagerFactory remoteStoreLockManagerFactory,
            ActionListener<String> listener
        ) {

        }

        @Override
        public Lifecycle.State lifecycleState() {
            return null;
        }

        @Override
        public void addLifecycleListener(LifecycleListener listener) {

        }

        @Override
        public void removeLifecycleListener(LifecycleListener listener) {

        }

        @Override
        public void start() {
            isStarted = true;
        }

        @Override
        public void stop() {

        }

        @Override
        public void close() {
            isClosed = true;
        }
    }

    private static class MeteredRepositoryTypeA extends MeteredBlobStoreRepository {
        private static final String TYPE = "type-a";
        private static final RepositoryStats STATS = new RepositoryStats(Map.of("GET", 10L));
        private final TestCryptoProvider cryptoHandler;

        private MeteredRepositoryTypeA(RepositoryMetadata metadata, ClusterService clusterService) {
            super(metadata, mock(NamedXContentRegistry.class), clusterService, mock(RecoverySettings.class), Map.of("bucket", "bucket-a"));

            if (metadata.cryptoMetadata() != null) {
                cryptoHandler = new TestCryptoProvider(
                    metadata.cryptoMetadata().keyProviderName(),
                    metadata.cryptoMetadata().keyProviderType()
                );
            } else {
                cryptoHandler = null;
            }
        }

        @Override
        protected BlobStore createBlobStore() {
            return mock(BlobStore.class);
        }

        @Override
        public RepositoryStats stats() {
            return STATS;
        }

        @Override
        public BlobPath basePath() {
            return BlobPath.cleanPath();
        }
    }

    private static class MeteredRepositoryTypeB extends MeteredBlobStoreRepository {
        private static final String TYPE = "type-b";
        private static final RepositoryStats STATS = new RepositoryStats(Map.of("LIST", 20L));
        private final TestCryptoProvider cryptoHandler;

        private MeteredRepositoryTypeB(RepositoryMetadata metadata, ClusterService clusterService) {
            super(metadata, mock(NamedXContentRegistry.class), clusterService, mock(RecoverySettings.class), Map.of("bucket", "bucket-b"));

            if (metadata.cryptoMetadata() != null) {
                cryptoHandler = new TestCryptoProvider(
                    metadata.cryptoMetadata().keyProviderName(),
                    metadata.cryptoMetadata().keyProviderType()
                );
            } else {
                cryptoHandler = null;
            }
        }

        @Override
        protected BlobStore createBlobStore() {
            return mock(BlobStore.class);
        }

        @Override
        public RepositoryStats stats() {
            return STATS;
        }

        @Override
        public BlobPath basePath() {
            return BlobPath.cleanPath();
        }
    }
}
