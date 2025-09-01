/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.index.store;

import org.apache.lucene.store.Directory;
import org.density.action.LatchedActionListener;
import org.density.cluster.metadata.IndexMetadata;
import org.density.common.blobstore.BlobContainer;
import org.density.common.blobstore.BlobMetadata;
import org.density.common.blobstore.BlobPath;
import org.density.common.blobstore.BlobStore;
import org.density.common.settings.Settings;
import org.density.core.action.ActionListener;
import org.density.core.index.shard.ShardId;
import org.density.index.IndexSettings;
import org.density.index.shard.ShardPath;
import org.density.repositories.RepositoriesService;
import org.density.repositories.RepositoryMissingException;
import org.density.repositories.blobstore.BlobStoreRepository;
import org.density.test.IndexSettingsModule;
import org.density.test.DensityTestCase;
import org.density.threadpool.ThreadPool;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Supplier;

import org.mockito.ArgumentCaptor;

import static org.density.index.store.RemoteSegmentStoreDirectory.METADATA_FILES_TO_FETCH;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RemoteSegmentStoreDirectoryFactoryTests extends DensityTestCase {

    private Supplier<RepositoriesService> repositoriesServiceSupplier;
    private RepositoriesService repositoriesService;
    private ThreadPool threadPool;
    private RemoteSegmentStoreDirectoryFactory remoteSegmentStoreDirectoryFactory;

    @Before
    public void setup() {
        repositoriesServiceSupplier = mock(Supplier.class);
        repositoriesService = mock(RepositoriesService.class);
        threadPool = mock(ThreadPool.class);
        when(repositoriesServiceSupplier.get()).thenReturn(repositoriesService);
        remoteSegmentStoreDirectoryFactory = new RemoteSegmentStoreDirectoryFactory(repositoriesServiceSupplier, threadPool, "");
    }

    public void testNewDirectory() throws IOException {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_INDEX_UUID, "uuid_1")
            .put(IndexMetadata.SETTING_REMOTE_SEGMENT_STORE_REPOSITORY, "remote_store_repository")
            .build();
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("foo", settings);
        Path tempDir = createTempDir().resolve(indexSettings.getUUID()).resolve("0");
        ShardPath shardPath = new ShardPath(false, tempDir, tempDir, new ShardId(indexSettings.getIndex(), 0));
        BlobStoreRepository repository = mock(BlobStoreRepository.class);
        BlobStore blobStore = mock(BlobStore.class);
        BlobContainer blobContainer = mock(BlobContainer.class);
        when(repository.blobStore()).thenReturn(blobStore);
        when(repository.basePath()).thenReturn(new BlobPath().add("base_path"));
        when(blobStore.blobContainer(any())).thenReturn(blobContainer);
        doAnswer(invocation -> {
            LatchedActionListener<List<BlobMetadata>> latchedActionListener = invocation.getArgument(3);
            latchedActionListener.onResponse(List.of());
            return null;
        }).when(blobContainer)
            .listBlobsByPrefixInSortedOrder(
                any(),
                eq(METADATA_FILES_TO_FETCH),
                eq(BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC),
                any(ActionListener.class)
            );

        when(repositoriesService.repository("remote_store_repository")).thenReturn(repository);

        try (Directory directory = remoteSegmentStoreDirectoryFactory.newDirectory(indexSettings, shardPath)) {
            assertTrue(directory instanceof RemoteSegmentStoreDirectory);
            ArgumentCaptor<BlobPath> blobPathCaptor = ArgumentCaptor.forClass(BlobPath.class);
            verify(blobStore, times(3)).blobContainer(blobPathCaptor.capture());
            List<BlobPath> blobPaths = blobPathCaptor.getAllValues();
            assertEquals("base_path/uuid_1/0/segments/data/", blobPaths.get(0).buildAsString());
            assertEquals("base_path/uuid_1/0/segments/metadata/", blobPaths.get(1).buildAsString());
            assertEquals("base_path/uuid_1/0/segments/lock_files/", blobPaths.get(2).buildAsString());

            verify(blobContainer).listBlobsByPrefixInSortedOrder(
                eq(RemoteSegmentStoreDirectory.MetadataFilenameUtils.METADATA_PREFIX),
                eq(METADATA_FILES_TO_FETCH),
                eq(BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC),
                any()
            );
            verify(repositoriesService, times(2)).repository("remote_store_repository");
        }
    }

    public void testNewDirectoryRepositoryDoesNotExist() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_REMOTE_SEGMENT_STORE_REPOSITORY, "remote_store_repository")
            .build();
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("foo", settings);
        Path tempDir = createTempDir().resolve(indexSettings.getUUID()).resolve("0");
        ShardPath shardPath = new ShardPath(false, tempDir, tempDir, new ShardId(indexSettings.getIndex(), 0));

        when(repositoriesService.repository("remote_store_repository")).thenThrow(new RepositoryMissingException("Missing"));

        assertThrows(IllegalArgumentException.class, () -> remoteSegmentStoreDirectoryFactory.newDirectory(indexSettings, shardPath));
    }

}
