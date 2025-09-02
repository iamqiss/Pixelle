/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.repositories.blobstore;

import org.density.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.density.cluster.metadata.IndexMetadata;
import org.density.cluster.service.ClusterService;
import org.density.common.blobstore.BlobContainer;
import org.density.common.blobstore.BlobPath;
import org.density.common.settings.Settings;
import org.density.core.common.Strings;
import org.density.core.xcontent.NamedXContentRegistry;
import org.density.env.Environment;
import org.density.index.IndexModule;
import org.density.index.IndexService;
import org.density.index.IndexSettings;
import org.density.index.store.RemoteBufferedOutputDirectory;
import org.density.indices.IndicesService;
import org.density.indices.RemoteStoreSettings;
import org.density.indices.recovery.RecoverySettings;
import org.density.indices.replication.common.ReplicationType;
import org.density.plugins.Plugin;
import org.density.plugins.RepositoryPlugin;
import org.density.repositories.RepositoriesService;
import org.density.repositories.Repository;
import org.density.repositories.fs.FsRepository;
import org.density.snapshots.SnapshotInfo;
import org.density.snapshots.SnapshotState;
import org.density.test.DensityIntegTestCase;
import org.density.test.DensitySingleNodeTestCase;
import org.density.transport.client.Client;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.density.index.remote.RemoteStoreEnums.DataCategory.SEGMENTS;
import static org.density.index.remote.RemoteStoreEnums.DataType.LOCK_FILES;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

public class BlobStoreRepositoryHelperTests extends DensitySingleNodeTestCase {

    static final String REPO_TYPE = "fsLike";

    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Arrays.asList(FsLikeRepoPlugin.class);
    }

    protected String[] getLockFilesInRemoteStore(String remoteStoreIndex, String remoteStoreRepository) throws IOException {
        final RepositoriesService repositoriesService = getInstanceFromNode(RepositoriesService.class);
        final BlobStoreRepository remoteStorerepository = (BlobStoreRepository) repositoriesService.repository(remoteStoreRepository);
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        String segmentsPathFixedPrefix = RemoteStoreSettings.CLUSTER_REMOTE_STORE_SEGMENTS_PATH_PREFIX.get(clusterService.getSettings());
        BlobPath shardLevelBlobPath = getShardLevelBlobPath(
            client(),
            remoteStoreIndex,
            remoteStorerepository.basePath(),
            "0",
            SEGMENTS,
            LOCK_FILES,
            segmentsPathFixedPrefix
        );
        BlobContainer blobContainer = remoteStorerepository.blobStore().blobContainer(shardLevelBlobPath);
        try (RemoteBufferedOutputDirectory lockDirectory = new RemoteBufferedOutputDirectory(blobContainer)) {
            return Arrays.stream(lockDirectory.listAll())
                .filter(lock -> lock.endsWith(".lock") || lock.endsWith(".v2_lock"))
                .toArray(String[]::new);
        }
    }

    // the reason for this plug-in is to drop any assertSnapshotOrGenericThread as mostly all access in this test goes from test threads
    public static class FsLikeRepoPlugin extends Plugin implements RepositoryPlugin {

        @Override
        public Map<String, Repository.Factory> getRepositories(
            Environment env,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            RecoverySettings recoverySettings
        ) {
            return Collections.singletonMap(
                REPO_TYPE,
                (metadata) -> new FsRepository(metadata, env, namedXContentRegistry, clusterService, recoverySettings) {
                    @Override
                    protected void assertSnapshotOrGenericThread() {
                        // eliminate thread name check as we access blobStore on test/main threads
                    }
                }
            );
        }
    }

    protected void createRepository(Client client, String repoName) {
        Settings.Builder settings = Settings.builder()
            .put(node().settings())
            .put("location", DensityIntegTestCase.randomRepoPath(node().settings()));
        DensityIntegTestCase.putRepository(client.admin().cluster(), repoName, REPO_TYPE, settings);
    }

    protected void createRepository(Client client, String repoName, Settings repoSettings) {
        Settings.Builder settingsBuilder = Settings.builder().put(repoSettings);
        DensityIntegTestCase.putRepository(client.admin().cluster(), repoName, REPO_TYPE, settingsBuilder);
    }

    protected void updateRepository(Client client, String repoName, Settings repoSettings) {
        createRepository(client, repoName, repoSettings);
    }

    protected Settings getRemoteStoreBackedIndexSettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, "1")
            .put("index.refresh_interval", "300s")
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, "1")
            .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), IndexModule.Type.FS.getSettingsKey())
            .put(IndexModule.INDEX_QUERY_CACHE_ENABLED_SETTING.getKey(), false)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .build();
    }

    protected SnapshotInfo createSnapshot(String repositoryName, String snapshot, List<String> indices) {
        logger.info("--> creating snapshot [{}] of {} in [{}]", snapshot, indices, repositoryName);

        final CreateSnapshotResponse response = client().admin()
            .cluster()
            .prepareCreateSnapshot(repositoryName, snapshot)
            .setIndices(indices.toArray(Strings.EMPTY_ARRAY))
            .setWaitForCompletion(true)
            .get();
        SnapshotInfo snapshotInfo = response.getSnapshotInfo();
        assertThat(snapshotInfo.state(), is(SnapshotState.SUCCESS));
        assertThat(snapshotInfo.successfulShards(), greaterThan(0));
        assertThat(snapshotInfo.failedShards(), equalTo(0));
        return snapshotInfo;
    }

    protected void indexDocuments(Client client, String indexName) {
        int numDocs = randomIntBetween(10, 20);
        for (int i = 0; i < numDocs; i++) {
            String id = Integer.toString(i);
            client.prepareIndex(indexName).setId(id).setSource("text", "sometext").get();
        }
    }

    protected IndexSettings getIndexSettings(String indexName) {
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexService indexService = indicesService.indexService(resolveIndex(indexName));
        return indexService.getIndexSettings();
    }

}
