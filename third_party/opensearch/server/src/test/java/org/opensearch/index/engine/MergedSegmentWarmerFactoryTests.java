/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.index.engine;

import org.apache.lucene.index.IndexWriter;
import org.density.cluster.metadata.IndexMetadata;
import org.density.cluster.service.ClusterService;
import org.density.common.settings.Settings;
import org.density.core.index.Index;
import org.density.core.index.shard.ShardId;
import org.density.index.IndexSettings;
import org.density.index.shard.IndexShard;
import org.density.indices.recovery.RecoverySettings;
import org.density.indices.replication.common.ReplicationType;
import org.density.test.IndexSettingsModule;
import org.density.test.DensityTestCase;
import org.density.transport.TransportService;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MergedSegmentWarmerFactoryTests extends DensityTestCase {

    private TransportService transportService;
    private RecoverySettings recoverySettings;
    private ClusterService clusterService;
    private MergedSegmentWarmerFactory factory;
    private IndexShard indexShard;
    private ShardId shardId;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        transportService = mock(TransportService.class);
        recoverySettings = mock(RecoverySettings.class);
        clusterService = mock(ClusterService.class);
        factory = new MergedSegmentWarmerFactory(transportService, recoverySettings, clusterService);

        indexShard = mock(IndexShard.class);
        shardId = new ShardId(new Index("test", "uuid"), 0);
        when(indexShard.shardId()).thenReturn(shardId);
    }

    public void testGetWithSegmentReplicationEnabled() {
        IndexSettings indexSettings = createIndexSettings(
            false,
            Settings.builder().put(IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.SEGMENT).build()
        );
        when(indexShard.indexSettings()).thenReturn(indexSettings);
        IndexWriter.IndexReaderWarmer warmer = factory.get(indexShard);

        assertNotNull(warmer);
        assertTrue(warmer instanceof MergedSegmentWarmer);
    }

    public void testGetWithRemoteStoreEnabled() {
        IndexSettings indexSettings = createIndexSettings(
            true,
            Settings.builder().put(IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.SEGMENT).build()
        );
        when(indexShard.indexSettings()).thenReturn(indexSettings);

        IndexWriter.IndexReaderWarmer warmer = factory.get(indexShard);

        assertNotNull(warmer);
        assertTrue(warmer instanceof MergedSegmentWarmer);
    }

    public void testGetWithDocumentReplication() {
        IndexSettings indexSettings = createIndexSettings(
            false,
            Settings.builder().put(IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.DOCUMENT).build()
        );

        when(indexShard.indexSettings()).thenReturn(indexSettings);
        IndexWriter.IndexReaderWarmer warmer = factory.get(indexShard);

        assertNull(warmer);
    }

    public static IndexSettings createIndexSettings(boolean remote, Settings settings) {
        IndexSettings indexSettings;
        if (remote) {
            Settings nodeSettings = Settings.builder()
                .put("node.name", "xyz")
                .put("node.attr.remote_store.translog.repository", "translog_repo")
                .put("node.attr.remote_store.segment.repository", "seg_repo")
                .put("node.attr.remote_store.enabled", "true")
                .build();
            indexSettings = IndexSettingsModule.newIndexSettings(new Index("test_index", "_na_"), settings, nodeSettings);
        } else {
            indexSettings = IndexSettingsModule.newIndexSettings("test_index", settings);
        }
        return indexSettings;
    }
}
