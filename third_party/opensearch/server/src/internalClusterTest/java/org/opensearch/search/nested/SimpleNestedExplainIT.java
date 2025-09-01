/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.search.nested;

import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.join.ScoreMode;
import org.density.action.search.SearchResponse;
import org.density.common.settings.Settings;
import org.density.test.DensityIntegTestCase;

import static org.density.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.density.common.xcontent.XContentFactory.jsonBuilder;
import static org.density.index.query.QueryBuilders.nestedQuery;
import static org.density.index.query.QueryBuilders.termQuery;
import static org.density.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.density.test.hamcrest.DensityAssertions.assertAcked;
import static org.density.test.hamcrest.DensityAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;

/**
 * Creating a separate class with no parameterization to create and index documents in a single
 * test run and compare search responses across concurrent and non-concurrent search. For more details,
 * refer: https://github.com/density-project/Density/issues/11413
 */
public class SimpleNestedExplainIT extends DensityIntegTestCase {

    @Override
    protected int numberOfShards() {
        return 1;
    }

    /*
     * Tests the explain output for multiple docs. Concurrent search with multiple slices is tested
     * here as call to indexRandomForMultipleSlices is made and compared with explain output for
     * non-concurrent search use-case. Separate test class is created to test explain for 1 slice
     * case in concurrent search, refer {@link SimpleExplainIT#testExplainWithSingleDoc}
     * For more details, refer: https://github.com/density-project/Density/issues/11413
     * */
    public void testExplainMultipleDocs() throws Exception {
        assertAcked(
            prepareCreate("test").setMapping(
                jsonBuilder().startObject()
                    .startObject("properties")
                    .startObject("nested1")
                    .field("type", "nested")
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );

        ensureGreen();

        client().prepareIndex("test")
            .setId("1")
            .setSource(
                jsonBuilder().startObject()
                    .field("field1", "value1")
                    .startArray("nested1")
                    .startObject()
                    .field("n_field1", "n_value1")
                    .endObject()
                    .startObject()
                    .field("n_field1", "n_value1")
                    .endObject()
                    .endArray()
                    .endObject()
            )
            .setRefreshPolicy(IMMEDIATE)
            .get();

        client().prepareIndex("test")
            .setId("2")
            .setSource(
                jsonBuilder().startObject()
                    .field("field1", "value2")
                    .startArray("nested1")
                    .startObject()
                    .field("n_field1", "n_value2")
                    .endObject()
                    .startObject()
                    .field("n_field1", "n_value2")
                    .endObject()
                    .endArray()
                    .endObject()
            )
            .setRefreshPolicy(IMMEDIATE)
            .get();

        // Turn off the concurrent search setting to test search with non-concurrent search
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), false).build())
            .get();

        SearchResponse nonConSearchResp = client().prepareSearch("test")
            .setQuery(nestedQuery("nested1", termQuery("nested1.n_field1", "n_value1"), ScoreMode.Total))
            .setExplain(true)
            .get();
        assertNoFailures(nonConSearchResp);
        assertThat(nonConSearchResp.getHits().getTotalHits().value(), equalTo(1L));
        Explanation nonConSearchExplain = nonConSearchResp.getHits().getHits()[0].getExplanation();
        assertThat(nonConSearchExplain.getValue(), equalTo(nonConSearchResp.getHits().getHits()[0].getScore()));

        // Turn on the concurrent search setting to test search with concurrent search
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), true).build())
            .get();

        SearchResponse conSearchResp = client().prepareSearch("test")
            .setQuery(nestedQuery("nested1", termQuery("nested1.n_field1", "n_value1"), ScoreMode.Total))
            .setExplain(true)
            .get();
        assertNoFailures(conSearchResp);
        assertThat(conSearchResp.getHits().getTotalHits().value(), equalTo(1L));
        Explanation conSearchExplain = conSearchResp.getHits().getHits()[0].getExplanation();
        assertThat(conSearchExplain.getValue(), equalTo(conSearchResp.getHits().getHits()[0].getScore()));

        // assert that the explanation for concurrent search should be equal to the non-concurrent search's explanation
        assertEquals(nonConSearchExplain, conSearchExplain);
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().putNull(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey()).build())
            .get();
    }
}
