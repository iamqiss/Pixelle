/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.gateway.remote.model;

import org.density.Version;
import org.density.cluster.metadata.IndexMetadata;
import org.density.cluster.routing.IndexRoutingTable;
import org.density.common.blobstore.BlobPath;
import org.density.common.compress.DeflateCompressor;
import org.density.common.settings.ClusterSettings;
import org.density.common.settings.Settings;
import org.density.core.index.Index;
import org.density.gateway.remote.RemoteClusterStateUtils;
import org.density.gateway.remote.routingtable.RemoteIndexRoutingTable;
import org.density.index.remote.RemoteStoreEnums;
import org.density.index.remote.RemoteStorePathStrategy;
import org.density.index.translog.transfer.BlobStoreTransferService;
import org.density.repositories.blobstore.BlobStoreRepository;
import org.density.test.DensityTestCase;
import org.density.threadpool.TestThreadPool;
import org.density.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import static org.density.gateway.remote.RemoteClusterStateUtils.CLUSTER_STATE_PATH_TOKEN;
import static org.density.gateway.remote.routingtable.RemoteIndexRoutingTable.INDEX_ROUTING_TABLE;
import static org.density.index.remote.RemoteStoreEnums.PathHashAlgorithm.FNV_1A_BASE64;
import static org.density.index.remote.RemoteStoreEnums.PathType.HASHED_PREFIX;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RemoteRoutingTableBlobStoreTests extends DensityTestCase {

    private RemoteRoutingTableBlobStore<IndexRoutingTable, RemoteIndexRoutingTable> remoteIndexRoutingTableStore;
    ClusterSettings clusterSettings;
    ThreadPool threadPool;

    @Before
    public void setup() {
        clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        BlobStoreTransferService blobStoreTransferService = mock(BlobStoreTransferService.class);
        BlobStoreRepository blobStoreRepository = mock(BlobStoreRepository.class);
        BlobPath blobPath = new BlobPath().add("base-path");
        when(blobStoreRepository.basePath()).thenReturn(blobPath);

        threadPool = new TestThreadPool(getClass().getName());
        this.remoteIndexRoutingTableStore = new RemoteRoutingTableBlobStore<>(
            blobStoreTransferService,
            blobStoreRepository,
            "test-cluster",
            threadPool,
            ThreadPool.Names.REMOTE_STATE_READ,
            clusterSettings
        );
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdown();

    }

    public void testRemoteRoutingTablePathTypeSetting() {
        // Assert the default is HASHED_PREFIX
        assertEquals(HASHED_PREFIX.toString(), remoteIndexRoutingTableStore.getPathTypeSetting().toString());

        Settings newSettings = Settings.builder()
            .put("cluster.remote_store.routing_table.path_type", RemoteStoreEnums.PathType.FIXED.toString())
            .build();
        clusterSettings.applySettings(newSettings);
        assertEquals(RemoteStoreEnums.PathType.FIXED.toString(), remoteIndexRoutingTableStore.getPathTypeSetting().toString());
    }

    public void testRemoteRoutingTableHashAlgoSetting() {
        // Assert the default is FNV_1A_BASE64
        assertEquals(FNV_1A_BASE64.toString(), remoteIndexRoutingTableStore.getPathHashAlgoSetting().toString());

        Settings newSettings = Settings.builder()
            .put("cluster.remote_store.routing_table.path_hash_algo", RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1.toString())
            .build();
        clusterSettings.applySettings(newSettings);
        assertEquals(
            RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1.toString(),
            remoteIndexRoutingTableStore.getPathHashAlgoSetting().toString()
        );
    }

    public void testGetBlobPathForUpload() {

        Index index = new Index("test-idx", "index-uuid");
        Settings idxSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
            .build();

        IndexMetadata indexMetadata = new IndexMetadata.Builder(index.getName()).settings(idxSettings)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        IndexRoutingTable indexRoutingTable = new IndexRoutingTable.Builder(index).initializeAsNew(indexMetadata).build();

        RemoteIndexRoutingTable remoteObjectForUpload = new RemoteIndexRoutingTable(
            indexRoutingTable,
            "cluster-uuid",
            new DeflateCompressor(),
            2L,
            3L
        );
        BlobPath blobPath = remoteIndexRoutingTableStore.getBlobPathForUpload(remoteObjectForUpload);
        BlobPath expectedPath = HASHED_PREFIX.path(
            RemoteStorePathStrategy.PathInput.builder()
                .basePath(
                    new BlobPath().add("base-path")
                        .add(RemoteClusterStateUtils.encodeString("test-cluster"))
                        .add(CLUSTER_STATE_PATH_TOKEN)
                        .add("cluster-uuid")
                        .add(INDEX_ROUTING_TABLE)
                )
                .indexUUID(index.getUUID())
                .build(),
            FNV_1A_BASE64
        );
        assertEquals(expectedPath, blobPath);
    }
}
