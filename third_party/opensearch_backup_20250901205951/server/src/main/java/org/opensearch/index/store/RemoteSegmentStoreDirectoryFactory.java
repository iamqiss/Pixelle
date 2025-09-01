/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.index.store;

import org.apache.lucene.store.Directory;
import org.density.common.annotation.PublicApi;
import org.density.common.blobstore.BlobPath;
import org.density.core.index.shard.ShardId;
import org.density.index.IndexSettings;
import org.density.index.remote.RemoteStorePathStrategy;
import org.density.index.shard.ShardPath;
import org.density.index.store.lockmanager.RemoteStoreLockManager;
import org.density.index.store.lockmanager.RemoteStoreLockManagerFactory;
import org.density.plugins.IndexStorePlugin;
import org.density.repositories.RepositoriesService;
import org.density.repositories.Repository;
import org.density.repositories.RepositoryMissingException;
import org.density.repositories.blobstore.BlobStoreRepository;
import org.density.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static org.density.index.remote.RemoteStoreEnums.DataCategory.SEGMENTS;
import static org.density.index.remote.RemoteStoreEnums.DataType.DATA;
import static org.density.index.remote.RemoteStoreEnums.DataType.METADATA;

/**
 * Factory for a remote store directory
 *
 * @density.api
 */
@PublicApi(since = "2.3.0")
public class RemoteSegmentStoreDirectoryFactory implements IndexStorePlugin.DirectoryFactory {
    private final Supplier<RepositoriesService> repositoriesService;
    private final String segmentsPathFixedPrefix;

    private final ThreadPool threadPool;

    public RemoteSegmentStoreDirectoryFactory(
        Supplier<RepositoriesService> repositoriesService,
        ThreadPool threadPool,
        String segmentsPathFixedPrefix
    ) {
        this.repositoriesService = repositoriesService;
        this.segmentsPathFixedPrefix = segmentsPathFixedPrefix;
        this.threadPool = threadPool;
    }

    @Override
    public Directory newDirectory(IndexSettings indexSettings, ShardPath path) throws IOException {
        String repositoryName = indexSettings.getRemoteStoreRepository();
        String indexUUID = indexSettings.getIndex().getUUID();
        return newDirectory(repositoryName, indexUUID, path.getShardId(), indexSettings.getRemoteStorePathStrategy());
    }

    public Directory newDirectory(String repositoryName, String indexUUID, ShardId shardId, RemoteStorePathStrategy pathStrategy)
        throws IOException {
        return newDirectory(repositoryName, indexUUID, shardId, pathStrategy, null);
    }

    public Directory newDirectory(
        String repositoryName,
        String indexUUID,
        ShardId shardId,
        RemoteStorePathStrategy pathStrategy,
        String indexFixedPrefix
    ) throws IOException {
        assert Objects.nonNull(pathStrategy);
        try (Repository repository = repositoriesService.get().repository(repositoryName)) {

            assert repository instanceof BlobStoreRepository : "repository should be instance of BlobStoreRepository";
            BlobStoreRepository blobStoreRepository = ((BlobStoreRepository) repository);
            BlobPath repositoryBasePath = blobStoreRepository.basePath();
            String shardIdStr = String.valueOf(shardId.id());
            Map<String, String> pendingDownloadMergedSegments = new ConcurrentHashMap<>();

            RemoteStorePathStrategy.ShardDataPathInput dataPathInput = RemoteStorePathStrategy.ShardDataPathInput.builder()
                .basePath(repositoryBasePath)
                .indexUUID(indexUUID)
                .shardId(shardIdStr)
                .dataCategory(SEGMENTS)
                .dataType(DATA)
                .fixedPrefix(segmentsPathFixedPrefix)
                .indexFixedPrefix(indexFixedPrefix)
                .build();
            // Derive the path for data directory of SEGMENTS
            BlobPath dataPath = pathStrategy.generatePath(dataPathInput);
            RemoteDirectory dataDirectory = new RemoteDirectory(
                blobStoreRepository.blobStore().blobContainer(dataPath),
                blobStoreRepository::maybeRateLimitRemoteUploadTransfers,
                blobStoreRepository::maybeRateLimitLowPriorityRemoteUploadTransfers,
                blobStoreRepository::maybeRateLimitRemoteDownloadTransfers,
                blobStoreRepository::maybeRateLimitLowPriorityDownloadTransfers,
                pendingDownloadMergedSegments
            );

            RemoteStorePathStrategy.ShardDataPathInput mdPathInput = RemoteStorePathStrategy.ShardDataPathInput.builder()
                .basePath(repositoryBasePath)
                .indexUUID(indexUUID)
                .shardId(shardIdStr)
                .dataCategory(SEGMENTS)
                .dataType(METADATA)
                .fixedPrefix(segmentsPathFixedPrefix)
                .indexFixedPrefix(indexFixedPrefix)
                .build();
            // Derive the path for metadata directory of SEGMENTS
            BlobPath mdPath = pathStrategy.generatePath(mdPathInput);
            RemoteDirectory metadataDirectory = new RemoteDirectory(blobStoreRepository.blobStore().blobContainer(mdPath));

            // The path for lock is derived within the RemoteStoreLockManagerFactory
            RemoteStoreLockManager mdLockManager = RemoteStoreLockManagerFactory.newLockManager(
                repositoriesService.get(),
                repositoryName,
                indexUUID,
                shardIdStr,
                pathStrategy,
                segmentsPathFixedPrefix,
                indexFixedPrefix
            );

            return new RemoteSegmentStoreDirectory(
                dataDirectory,
                metadataDirectory,
                mdLockManager,
                threadPool,
                shardId,
                pendingDownloadMergedSegments
            );
        } catch (RepositoryMissingException e) {
            throw new IllegalArgumentException("Repository should be created before creating index with remote_store enabled setting", e);
        }
    }

    public Supplier<RepositoriesService> getRepositoriesService() {
        return this.repositoriesService;
    }

}
