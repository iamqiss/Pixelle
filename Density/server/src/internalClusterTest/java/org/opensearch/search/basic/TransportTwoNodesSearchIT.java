/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright Density Contributors. See
 * GitHub history for details.
 */

package org.density.search.basic;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.density.DensityException;
import org.density.action.search.MultiSearchResponse;
import org.density.action.search.SearchPhaseExecutionException;
import org.density.action.search.SearchResponse;
import org.density.common.settings.Settings;
import org.density.common.unit.TimeValue;
import org.density.core.xcontent.XContentBuilder;
import org.density.index.query.MatchQueryBuilder;
import org.density.index.query.QueryBuilders;
import org.density.index.query.functionscore.ScriptScoreFunctionBuilder;
import org.density.script.Script;
import org.density.script.ScriptType;
import org.density.search.SearchHit;
import org.density.search.aggregations.AggregationBuilders;
import org.density.search.aggregations.bucket.filter.Filter;
import org.density.search.aggregations.bucket.global.Global;
import org.density.search.builder.SearchSourceBuilder;
import org.density.search.sort.SortOrder;
import org.density.test.ParameterizedStaticSettingsDensityIntegTestCase;
import org.density.transport.client.Requests;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

import static org.density.action.search.SearchType.DFS_QUERY_THEN_FETCH;
import static org.density.action.search.SearchType.QUERY_THEN_FETCH;
import static org.density.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.density.common.xcontent.XContentFactory.jsonBuilder;
import static org.density.index.query.QueryBuilders.matchAllQuery;
import static org.density.index.query.QueryBuilders.termQuery;
import static org.density.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.density.search.builder.SearchSourceBuilder.searchSource;
import static org.density.test.hamcrest.DensityAssertions.assertNoFailures;
import static org.density.transport.client.Requests.createIndexRequest;
import static org.density.transport.client.Requests.searchRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public class TransportTwoNodesSearchIT extends ParameterizedStaticSettingsDensityIntegTestCase {

    public TransportTwoNodesSearchIT(Settings staticSettings) {
        super(staticSettings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), false).build() },
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), true).build() }
        );
    }

    @Override
    protected int numberOfReplicas() {
        return 0;
    }

    private Set<String> prepareData() throws Exception {
        return prepareData(-1);
    }

    private Set<String> prepareData(int numShards) throws Exception {
        Set<String> fullExpectedIds = new TreeSet<>();

        Settings.Builder settingsBuilder = Settings.builder().put(indexSettings());

        if (numShards > 0) {
            settingsBuilder.put(SETTING_NUMBER_OF_SHARDS, numShards);
        }

        client().admin()
            .indices()
            .create(createIndexRequest("test").settings(settingsBuilder).simpleMapping("foo", "type=geo_point"))
            .actionGet();

        ensureGreen();
        for (int i = 0; i < 100; i++) {
            index(Integer.toString(i), "test", i);
            fullExpectedIds.add(Integer.toString(i));
        }
        refresh();
        indexRandomForConcurrentSearch("test");
        return fullExpectedIds;
    }

    private void index(String id, String nameValue, int age) throws IOException {
        client().index(Requests.indexRequest("test").id(id).source(source(id, nameValue, age))).actionGet();
    }

    private XContentBuilder source(String id, String nameValue, int age) throws IOException {
        StringBuilder multi = new StringBuilder().append(nameValue);
        for (int i = 0; i < age; i++) {
            multi.append(" ").append(nameValue);
        }
        return jsonBuilder().startObject()
            .field("id", id)
            .field("nid", Integer.parseInt(id))
            .field("name", nameValue + id)
            .field("age", age)
            .field("multi", multi.toString())
            .endObject();
    }

    public void testDfsQueryThenFetch() throws Exception {
        Settings.Builder settingsBuilder = Settings.builder().put(indexSettings());
        client().admin().indices().create(createIndexRequest("test").settings(settingsBuilder)).actionGet();
        ensureGreen();

        // we need to have age (ie number of repeats of "test" term) high enough
        // to produce the same 8-bit norm for all docs here, so that
        // the tf is basically the entire score (assuming idf is fixed, which
        // it should be if dfs is working correctly)
        // With the current way of encoding norms, every length between 1048 and 1176
        // are encoded into the same byte
        for (int i = 1048; i < 1148; i++) {
            index(Integer.toString(i - 1048), "test", i);
        }
        refresh();

        int total = 0;
        SearchResponse searchResponse = client().prepareSearch("test")
            .setSearchType(DFS_QUERY_THEN_FETCH)
            .setQuery(termQuery("multi", "test"))
            .setSize(60)
            .setExplain(true)
            .setScroll(TimeValue.timeValueSeconds(30))
            .get();
        while (true) {
            assertNoFailures(searchResponse);
            assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(100L));
            SearchHit[] hits = searchResponse.getHits().getHits();
            if (hits.length == 0) {
                break; // finished
            }
            for (int i = 0; i < hits.length; ++i) {
                SearchHit hit = hits[i];
                assertThat(hit.getExplanation(), notNullValue());
                assertThat(hit.getExplanation().getDetails().length, equalTo(1));
                assertThat(hit.getExplanation().getDetails()[0].getDetails().length, equalTo(2));
                assertThat(hit.getExplanation().getDetails()[0].getDetails()[0].getDescription(), startsWith("idf"));
                assertThat(hit.getExplanation().getDetails()[0].getDetails()[0].getDetails().length, equalTo(2));
                assertThat(hit.getExplanation().getDetails()[0].getDetails()[0].getDetails()[0].getValue(), equalTo(100L));
                assertThat(hit.getExplanation().getDetails()[0].getDetails()[0].getDetails()[1].getValue(), equalTo(100L));
                assertThat(hit.getExplanation().getDetails()[0].getDetails()[1].getDescription(), containsString("freq"));
                assertThat(
                    "id[" + hit.getId() + "] -> " + hit.getExplanation().toString(),
                    hit.getId(),
                    equalTo(Integer.toString(100 - total - i - 1))
                );
            }
            total += hits.length;
            searchResponse = client().prepareSearchScroll(searchResponse.getScrollId()).setScroll(TimeValue.timeValueSeconds(30)).get();
        }
        clearScroll(searchResponse.getScrollId());
        assertEquals(100, total);
    }

    public void testDfsQueryThenFetchWithSort() throws Exception {
        prepareData();

        int total = 0;
        SearchResponse searchResponse = client().prepareSearch("test")
            .setSearchType(DFS_QUERY_THEN_FETCH)
            .setQuery(termQuery("multi", "test"))
            .setSize(60)
            .setExplain(true)
            .addSort("age", SortOrder.ASC)
            .setScroll(TimeValue.timeValueSeconds(30))
            .get();
        while (true) {
            assertNoFailures(searchResponse);
            assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(100L));
            SearchHit[] hits = searchResponse.getHits().getHits();
            if (hits.length == 0) {
                break; // finished
            }
            for (int i = 0; i < hits.length; ++i) {
                SearchHit hit = hits[i];
                assertThat(hit.getExplanation(), notNullValue());
                assertThat(hit.getExplanation().getDetails().length, equalTo(1));
                assertThat(hit.getExplanation().getDetails()[0].getDetails().length, equalTo(2));
                assertThat(hit.getExplanation().getDetails()[0].getDetails()[0].getDescription(), startsWith("idf"));
                assertThat(hit.getExplanation().getDetails()[0].getDetails()[0].getDetails().length, equalTo(2));
                assertThat(hit.getExplanation().getDetails()[0].getDetails()[0].getDetails()[0].getValue(), equalTo(100L));
                assertThat(hit.getExplanation().getDetails()[0].getDetails()[0].getDetails()[1].getValue(), equalTo(100L));
                assertThat(hit.getExplanation().getDetails()[0].getDetails()[1].getDescription(), containsString("freq"));
                assertThat("id[" + hit.getId() + "]", hit.getId(), equalTo(Integer.toString(total + i)));
            }
            total += hits.length;
            searchResponse = client().prepareSearchScroll(searchResponse.getScrollId()).setScroll(TimeValue.timeValueSeconds(30)).get();
        }
        clearScroll(searchResponse.getScrollId());
        assertEquals(100, total);
    }

    public void testQueryThenFetch() throws Exception {
        prepareData();

        int total = 0;
        SearchResponse searchResponse = client().prepareSearch("test")
            .setSearchType(QUERY_THEN_FETCH)
            .setQuery(termQuery("multi", "test"))
            .setSize(60)
            .setExplain(true)
            .addSort("nid", SortOrder.DESC)
            .setScroll(TimeValue.timeValueSeconds(30))
            .get();
        while (true) {
            assertNoFailures(searchResponse);
            assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(100L));
            SearchHit[] hits = searchResponse.getHits().getHits();
            if (hits.length == 0) {
                break; // finished
            }
            for (int i = 0; i < hits.length; ++i) {
                SearchHit hit = hits[i];
                assertThat(hit.getExplanation(), notNullValue());
                assertThat("id[" + hit.getId() + "]", hit.getId(), equalTo(Integer.toString(100 - total - i - 1)));
            }
            total += hits.length;
            searchResponse = client().prepareSearchScroll(searchResponse.getScrollId()).setScroll(TimeValue.timeValueSeconds(30)).get();
        }
        clearScroll(searchResponse.getScrollId());
        assertEquals(100, total);
    }

    public void testQueryThenFetchWithFrom() throws Exception {
        Set<String> fullExpectedIds = prepareData();

        SearchSourceBuilder source = searchSource().query(matchAllQuery()).explain(true);

        Set<String> collectedIds = new TreeSet<>();

        SearchResponse searchResponse = client().search(searchRequest("test").source(source.from(0).size(60)).searchType(QUERY_THEN_FETCH))
            .actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(100L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(60));
        for (int i = 0; i < 60; i++) {
            SearchHit hit = searchResponse.getHits().getHits()[i];
            collectedIds.add(hit.getId());
        }
        searchResponse = client().search(searchRequest("test").source(source.from(60).size(60)).searchType(QUERY_THEN_FETCH)).actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(100L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(40));
        for (int i = 0; i < 40; i++) {
            SearchHit hit = searchResponse.getHits().getHits()[i];
            collectedIds.add(hit.getId());
        }
        assertThat(collectedIds, equalTo(fullExpectedIds));
    }

    public void testQueryThenFetchWithSort() throws Exception {
        prepareData();

        int total = 0;
        SearchResponse searchResponse = client().prepareSearch("test")
            .setQuery(termQuery("multi", "test"))
            .setSize(60)
            .setExplain(true)
            .addSort("age", SortOrder.ASC)
            .setScroll(TimeValue.timeValueSeconds(30))
            .get();
        while (true) {
            assertNoFailures(searchResponse);
            assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(100L));
            SearchHit[] hits = searchResponse.getHits().getHits();
            if (hits.length == 0) {
                break; // finished
            }
            for (int i = 0; i < hits.length; ++i) {
                SearchHit hit = hits[i];
                assertThat(hit.getExplanation(), notNullValue());
                assertThat("id[" + hit.getId() + "]", hit.getId(), equalTo(Integer.toString(total + i)));
            }
            total += hits.length;
            searchResponse = client().prepareSearchScroll(searchResponse.getScrollId()).setScroll(TimeValue.timeValueSeconds(30)).get();
        }
        clearScroll(searchResponse.getScrollId());
        assertEquals(100, total);
    }

    public void testSimpleFacets() throws Exception {
        prepareData();

        SearchSourceBuilder sourceBuilder = searchSource().query(termQuery("multi", "test"))
            .from(0)
            .size(20)
            .explain(true)
            .aggregation(AggregationBuilders.global("global").subAggregation(AggregationBuilders.filter("all", termQuery("multi", "test"))))
            .aggregation(AggregationBuilders.filter("test1", termQuery("name", "test1")));

        SearchResponse searchResponse = client().search(searchRequest("test").source(sourceBuilder)).actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(100L));

        Global global = searchResponse.getAggregations().get("global");
        Filter all = global.getAggregations().get("all");
        Filter test1 = searchResponse.getAggregations().get("test1");
        assertThat(test1.getDocCount(), equalTo(1L));
        assertThat(all.getDocCount(), equalTo(100L));
    }

    public void testFailedSearchWithWrongQuery() throws Exception {
        prepareData();

        NumShards test = getNumShards("test");

        logger.info("Start Testing failed search with wrong query");
        try {
            SearchResponse searchResponse = client().search(
                searchRequest("test").source(new SearchSourceBuilder().query(new MatchQueryBuilder("foo", "biz")))
            ).actionGet();
            assertThat(searchResponse.getTotalShards(), equalTo(test.numPrimaries));
            assertThat(searchResponse.getSuccessfulShards(), equalTo(0));
            assertThat(searchResponse.getFailedShards(), equalTo(test.numPrimaries));
            fail("search should fail");
        } catch (DensityException e) {
            assertThat(e.unwrapCause(), instanceOf(SearchPhaseExecutionException.class));
            // all is well
        }
        logger.info("Done Testing failed search");
    }

    public void testFailedSearchWithWrongFrom() throws Exception {
        prepareData();

        NumShards test = getNumShards("test");

        logger.info("Start Testing failed search with wrong from");
        SearchSourceBuilder source = searchSource().query(termQuery("multi", "test")).from(1000).size(20).explain(true);
        SearchResponse response = client().search(searchRequest("test").searchType(DFS_QUERY_THEN_FETCH).source(source)).actionGet();
        assertThat(response.getHits().getHits().length, equalTo(0));
        assertThat(response.getTotalShards(), equalTo(test.numPrimaries));
        assertThat(response.getSuccessfulShards(), equalTo(test.numPrimaries));
        assertThat(response.getFailedShards(), equalTo(0));

        response = client().search(searchRequest("test").searchType(QUERY_THEN_FETCH).source(source)).actionGet();
        assertNoFailures(response);
        assertThat(response.getHits().getHits().length, equalTo(0));

        response = client().search(searchRequest("test").searchType(DFS_QUERY_THEN_FETCH).source(source)).actionGet();
        assertNoFailures(response);
        assertThat(response.getHits().getHits().length, equalTo(0));

        response = client().search(searchRequest("test").searchType(DFS_QUERY_THEN_FETCH).source(source)).actionGet();
        assertNoFailures(response);
        assertThat(response.getHits().getHits().length, equalTo(0));

        logger.info("Done Testing failed search");
    }

    public void testFailedMultiSearchWithWrongQuery() throws Exception {
        prepareData();

        logger.info("Start Testing failed multi search with a wrong query");

        MultiSearchResponse response = client().prepareMultiSearch()
            .add(client().prepareSearch("test").setQuery(new MatchQueryBuilder("foo", "biz")))
            .add(client().prepareSearch("test").setQuery(QueryBuilders.termQuery("nid", 2)))
            .add(client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery()))
            .get();
        assertThat(response.getResponses().length, equalTo(3));
        assertThat(response.getResponses()[0].getFailureMessage(), notNullValue());

        assertThat(response.getResponses()[1].getFailureMessage(), nullValue());
        assertThat(response.getResponses()[1].getResponse().getHits().getHits().length, equalTo(1));

        assertThat(response.getResponses()[2].getFailureMessage(), nullValue());
        assertThat(response.getResponses()[2].getResponse().getHits().getHits().length, equalTo(10));

        logger.info("Done Testing failed search");
    }

    public void testFailedMultiSearchWithWrongQueryWithFunctionScore() throws Exception {
        prepareData();

        logger.info("Start Testing failed multi search with a wrong query");

        MultiSearchResponse response = client().prepareMultiSearch()
            // Add custom score query with bogus script
            .add(
                client().prepareSearch("test")
                    .setQuery(
                        QueryBuilders.functionScoreQuery(
                            QueryBuilders.termQuery("nid", 1),
                            new ScriptScoreFunctionBuilder(new Script(ScriptType.INLINE, "bar", "foo", Collections.emptyMap()))
                        )
                    )
            )
            .add(client().prepareSearch("test").setQuery(QueryBuilders.termQuery("nid", 2)))
            .add(client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery()))
            .get();
        assertThat(response.getResponses().length, equalTo(3));
        assertThat(response.getResponses()[0].getFailureMessage(), notNullValue());

        assertThat(response.getResponses()[1].getFailureMessage(), nullValue());
        assertThat(response.getResponses()[1].getResponse().getHits().getHits().length, equalTo(1));

        assertThat(response.getResponses()[2].getFailureMessage(), nullValue());
        assertThat(response.getResponses()[2].getResponse().getHits().getHits().length, equalTo(10));

        logger.info("Done Testing failed search");
    }
}
