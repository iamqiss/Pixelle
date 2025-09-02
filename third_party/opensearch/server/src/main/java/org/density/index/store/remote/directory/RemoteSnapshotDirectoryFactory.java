/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.index.store.remote.directory;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.density.common.blobstore.BlobContainer;
import org.density.index.IndexSettings;
import org.density.index.remote.RemoteStoreEnums.PathType;
import org.density.index.shard.ShardPath;
import org.density.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.density.index.snapshots.blobstore.IndexShardSnapshot;
import org.density.index.store.remote.filecache.FileCache;
import org.density.index.store.remote.utils.TransferManager;
import org.density.plugins.IndexStorePlugin;
import org.density.repositories.IndexId;
import org.density.repositories.RepositoriesService;
import org.density.repositories.Repository;
import org.density.repositories.blobstore.BlobStoreRepository;
import org.density.snapshots.SnapshotId;
import org.density.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Supplier;

/**
 * Factory for a Directory implementation that can read directly from index
 * data stored remotely in a blob store repository.
 *
 * @density.internal
 */
public final class RemoteSnapshotDirectoryFactory implements IndexStorePlugin.DirectoryFactory {
    public static final String LOCAL_STORE_LOCATION = "RemoteLocalStore";

    private final Supplier<RepositoriesService> repositoriesService;
    private final ThreadPool threadPool;

    private final FileCache remoteStoreFileCache;

    public RemoteSnapshotDirectoryFactory(
        Supplier<RepositoriesService> repositoriesService,
        ThreadPool threadPool,
        FileCache remoteStoreFileCache
    ) {
        this.repositoriesService = repositoriesService;
        this.threadPool = threadPool;
        this.remoteStoreFileCache = remoteStoreFileCache;
    }

    @Override
    public Directory newDirectory(IndexSettings indexSettings, ShardPath localShardPath) throws IOException {
        final String repositoryName = IndexSettings.SEARCHABLE_SNAPSHOT_REPOSITORY.get(indexSettings.getSettings());
        final Repository repository = repositoriesService.get().repository(repositoryName);
        assert repository instanceof BlobStoreRepository : "repository should be instance of BlobStoreRepository";
        final BlobStoreRepository blobStoreRepository = (BlobStoreRepository) repository;
        try {
            return createRemoteSnapshotDirectoryFromSnapshot(indexSettings, localShardPath, blobStoreRepository).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    private Future<RemoteSnapshotDirectory> createRemoteSnapshotDirectoryFromSnapshot(
        IndexSettings indexSettings,
        ShardPath localShardPath,
        BlobStoreRepository blobStoreRepository
    ) throws IOException {
        // The below information like the snapshot generated indexId, shard_path_type and shardId are used for
        // creating the shard BlobContainer. This information has been updated as per the hashed_prefix snapshots.
        String indexId = IndexSettings.SEARCHABLE_SNAPSHOT_INDEX_ID.get(indexSettings.getSettings());
        PathType pathType = IndexSettings.SEARCHABLE_SNAPSHOT_SHARD_PATH_TYPE.get(indexSettings.getSettings());
        int shardId = localShardPath.getShardId().getId();
        final SnapshotId snapshotId = new SnapshotId(
            IndexSettings.SEARCHABLE_SNAPSHOT_ID_NAME.get(indexSettings.getSettings()),
            IndexSettings.SEARCHABLE_SNAPSHOT_ID_UUID.get(indexSettings.getSettings())
        );
        Path localStorePath = localShardPath.getDataPath().resolve(LOCAL_STORE_LOCATION);
        FSDirectory localStoreDir = FSDirectory.open(Files.createDirectories(localStorePath));
        // make sure directory is flushed to persistent storage
        localStoreDir.syncMetaData();
        // this trick is needed to bypass assertions in BlobStoreRepository::assertAllowableThreadPools in case of node restart and a remote
        // index restore is invoked
        return threadPool.executor(ThreadPool.Names.SNAPSHOT).submit(() -> {
            // shardContainer(IndexId, shardId) method uses the id and pathType information to generate the blobPath and
            // hence the blobContainer. We have used a dummy name as it plays no relevance in the blobPath generation.
            final BlobContainer blobContainer = blobStoreRepository.shardContainer(
                new IndexId("DUMMY", indexId, pathType.getCode()),
                shardId
            );
            final IndexShardSnapshot indexShardSnapshot = blobStoreRepository.loadShardSnapshot(blobContainer, snapshotId);
            assert indexShardSnapshot instanceof BlobStoreIndexShardSnapshot
                : "indexShardSnapshot should be an instance of BlobStoreIndexShardSnapshot";
            final BlobStoreIndexShardSnapshot snapshot = (BlobStoreIndexShardSnapshot) indexShardSnapshot;
            TransferManager transferManager = new TransferManager(blobContainer::readBlob, remoteStoreFileCache, threadPool);
            return new RemoteSnapshotDirectory(snapshot, localStoreDir, transferManager);
        });
    }
}
