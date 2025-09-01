/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.cluster.metadata;

import org.density.action.search.SearchResponse;
import org.density.action.support.WriteRequest;
import org.density.common.settings.Settings;
import org.density.remotestore.RemoteStoreBaseIntegTestCase;
import org.density.test.DensityIntegTestCase;

import java.util.concurrent.TimeUnit;

import static org.density.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.density.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SEARCH_REPLICAS;
import static org.density.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.density.test.hamcrest.DensityAssertions.assertAcked;
import static org.density.test.hamcrest.DensityAssertions.assertHitCount;

@DensityIntegTestCase.ClusterScope(scope = DensityIntegTestCase.Scope.TEST, numDataNodes = 0)
public class MetadataIndexStateServiceIT extends RemoteStoreBaseIntegTestCase {

    private static final String TEST_INDEX = "test_open_close_index";

    public void testIndexCloseAndOpen() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);

        Settings specificSettings = Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 1).build();

        createIndex(TEST_INDEX, specificSettings);
        ensureGreen(TEST_INDEX);

        for (int i = 0; i < 10; i++) {
            client().prepareIndex(TEST_INDEX)
                .setId(Integer.toString(i))
                .setSource("field1", "value" + i)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();
        }

        assertAcked(client().admin().indices().prepareClose(TEST_INDEX).get());
        assertEquals(
            IndexMetadata.State.CLOSE,
            client().admin().cluster().prepareState().get().getState().metadata().index(TEST_INDEX).getState()
        );

        assertAcked(client().admin().indices().prepareOpen(TEST_INDEX).get());
        ensureGreen(TEST_INDEX);

        assertBusy(() -> {
            SearchResponse searchResponse = client().prepareSearch(TEST_INDEX).get();
            assertHitCount(searchResponse, 10);
        }, 30, TimeUnit.SECONDS);
    }

    public void testIndexCloseAndOpenWithSearchOnlyMode() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        internalCluster().startSearchOnlyNodes(1);

        Settings specificSettings = Settings.builder()
            .put(SETTING_NUMBER_OF_SHARDS, 1)
            .put(SETTING_NUMBER_OF_REPLICAS, 1)
            .put(SETTING_NUMBER_OF_SEARCH_REPLICAS, 1)
            .build();

        createIndex(TEST_INDEX, specificSettings);
        ensureGreen(TEST_INDEX);

        for (int i = 0; i < 10; i++) {
            client().prepareIndex(TEST_INDEX)
                .setId(Integer.toString(i))
                .setSource("field1", "value" + i)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();
        }

        assertAcked(client().admin().indices().prepareScaleSearchOnly(TEST_INDEX, true).get());
        ensureGreen(TEST_INDEX);

        assertTrue(
            client().admin()
                .indices()
                .prepareGetSettings(TEST_INDEX)
                .get()
                .getSetting(TEST_INDEX, IndexMetadata.INDEX_BLOCKS_SEARCH_ONLY_SETTING.getKey())
                .equals("true")
        );

        assertAcked(client().admin().indices().prepareClose(TEST_INDEX).get());
        assertEquals(
            IndexMetadata.State.CLOSE,
            client().admin().cluster().prepareState().get().getState().metadata().index(TEST_INDEX).getState()
        );

        assertAcked(client().admin().indices().prepareOpen(TEST_INDEX).get());
        ensureGreen(TEST_INDEX);

        assertBusy(() -> {
            SearchResponse searchResponse = client().prepareSearch(TEST_INDEX).get();
            assertHitCount(searchResponse, 10);
        }, 30, TimeUnit.SECONDS);
    }
}
