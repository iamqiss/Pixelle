/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.index.store.lockmanager;

import org.density.common.annotation.PublicApi;
import org.density.common.blobstore.BlobContainer;
import org.density.common.blobstore.BlobPath;
import org.density.index.remote.RemoteStorePathStrategy;
import org.density.index.store.RemoteBufferedOutputDirectory;
import org.density.repositories.RepositoriesService;
import org.density.repositories.Repository;
import org.density.repositories.RepositoryMissingException;
import org.density.repositories.blobstore.BlobStoreRepository;

import java.util.function.Supplier;

import static org.density.index.remote.RemoteStoreEnums.DataCategory.SEGMENTS;
import static org.density.index.remote.RemoteStoreEnums.DataType.LOCK_FILES;

/**
 * Factory for remote store lock manager
 *
 * @density.api
 */
@PublicApi(since = "2.8.0")
public class RemoteStoreLockManagerFactory {
    private final Supplier<RepositoriesService> repositoriesService;
    private final String segmentsPathFixedPrefix;

    public RemoteStoreLockManagerFactory(Supplier<RepositoriesService> repositoriesService, String segmentsPathFixedPrefix) {
        this.repositoriesService = repositoriesService;
        this.segmentsPathFixedPrefix = segmentsPathFixedPrefix;
    }

    public RemoteStoreLockManager newLockManager(
        String repositoryName,
        String indexUUID,
        String shardId,
        RemoteStorePathStrategy pathStrategy
    ) {
        return newLockManager(repositoriesService.get(), repositoryName, indexUUID, shardId, pathStrategy, segmentsPathFixedPrefix, null);
    }

    public static RemoteStoreMetadataLockManager newLockManager(
        RepositoriesService repositoriesService,
        String repositoryName,
        String indexUUID,
        String shardId,
        RemoteStorePathStrategy pathStrategy,
        String segmentsPathFixedPrefix
    ) {
        return newLockManager(repositoriesService, repositoryName, indexUUID, shardId, pathStrategy, segmentsPathFixedPrefix, null);
    }

    public static RemoteStoreMetadataLockManager newLockManager(
        RepositoriesService repositoriesService,
        String repositoryName,
        String indexUUID,
        String shardId,
        RemoteStorePathStrategy pathStrategy,
        String segmentsPathFixedPrefix,
        String indexFixedPrefix
    ) {
        try (Repository repository = repositoriesService.repository(repositoryName)) {
            assert repository instanceof BlobStoreRepository : "repository should be instance of BlobStoreRepository";
            BlobPath repositoryBasePath = ((BlobStoreRepository) repository).basePath();

            RemoteStorePathStrategy.ShardDataPathInput lockFilesPathInput = RemoteStorePathStrategy.ShardDataPathInput.builder()
                .basePath(repositoryBasePath)
                .indexUUID(indexUUID)
                .shardId(shardId)
                .dataCategory(SEGMENTS)
                .dataType(LOCK_FILES)
                .fixedPrefix(segmentsPathFixedPrefix)
                .indexFixedPrefix(indexFixedPrefix)
                .build();
            BlobPath lockDirectoryPath = pathStrategy.generatePath(lockFilesPathInput);
            BlobContainer lockDirectoryBlobContainer = ((BlobStoreRepository) repository).blobStore().blobContainer(lockDirectoryPath);
            return new RemoteStoreMetadataLockManager(new RemoteBufferedOutputDirectory(lockDirectoryBlobContainer));
        } catch (RepositoryMissingException e) {
            throw new IllegalArgumentException("Repository should be present to acquire/release lock", e);
        }
    }

    // TODO: remove this once we add poller in place to trigger remote store cleanup
    // see: https://github.com/density-project/Density/issues/8469
    public Supplier<RepositoriesService> getRepositoriesService() {
        return repositoriesService;
    }
}
