/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.index.remote;

import org.density.cluster.metadata.IndexMetadata;
import org.density.common.settings.Settings;
import org.density.core.index.Index;
import org.density.core.index.shard.ShardId;
import org.density.index.IndexSettings;
import org.density.index.shard.IndexShard;
import org.density.index.store.Store;
import org.density.indices.replication.common.ReplicationType;
import org.density.test.IndexSettingsModule;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Helper functions for Remote Store tests
 */
public class RemoteStoreTestsHelper {
    static IndexShard createIndexShard(ShardId shardId, boolean remoteStoreEnabled) {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, String.valueOf(remoteStoreEnabled))
            .build();
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test_index", settings);
        Store store = mock(Store.class);
        IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.indexSettings()).thenReturn(indexSettings);
        when(indexShard.shardId()).thenReturn(shardId);
        when(indexShard.store()).thenReturn(store);
        return indexShard;
    }

    public static IndexSettings createIndexSettings(boolean remote) {
        return createIndexSettings(remote, Settings.EMPTY);
    }

    public static IndexSettings createIndexSettings(boolean remote, Settings settings) {
        IndexSettings indexSettings;
        if (remote) {
            Settings nodeSettings = Settings.builder()
                .put("node.name", "xyz")
                .put("node.attr.remote_store.translog.repository", "seg_repo")
                .build();
            indexSettings = IndexSettingsModule.newIndexSettings(new Index("test_index", "_na_"), settings, nodeSettings);
        } else {
            indexSettings = IndexSettingsModule.newIndexSettings("test_index", settings);
        }
        return indexSettings;
    }
}
