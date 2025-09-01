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

package org.density.index.search;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.density.action.admin.indices.create.CreateIndexRequestBuilder;
import org.density.action.index.IndexRequestBuilder;
import org.density.action.search.SearchResponse;
import org.density.common.settings.Settings;
import org.density.index.query.MatchPhraseQueryBuilder;
import org.density.index.search.MatchQuery.ZeroTermsQuery;
import org.density.test.ParameterizedStaticSettingsDensityIntegTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.density.index.query.QueryBuilders.matchPhraseQuery;
import static org.density.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.density.test.hamcrest.DensityAssertions.assertAcked;
import static org.density.test.hamcrest.DensityAssertions.assertHitCount;

public class MatchPhraseQueryIT extends ParameterizedStaticSettingsDensityIntegTestCase {

    private static final String INDEX = "test";

    public MatchPhraseQueryIT(Settings staticSettings) {
        super(staticSettings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), false).build() },
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), true).build() }
        );
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        CreateIndexRequestBuilder createIndexRequest = prepareCreate(INDEX).setSettings(
            Settings.builder()
                .put(indexSettings())
                .put("index.analysis.analyzer.standard_stopwords.type", "standard")
                .putList("index.analysis.analyzer.standard_stopwords.stopwords", "of", "the", "who")
        );
        assertAcked(createIndexRequest);
        ensureGreen();
    }

    public void testZeroTermsQuery() throws ExecutionException, InterruptedException {
        List<IndexRequestBuilder> indexRequests = getIndexRequests();
        indexRandom(true, false, indexRequests);

        MatchPhraseQueryBuilder baseQuery = matchPhraseQuery("name", "the who").analyzer("standard_stopwords");

        MatchPhraseQueryBuilder matchNoneQuery = baseQuery.zeroTermsQuery(ZeroTermsQuery.NONE);
        SearchResponse matchNoneResponse = client().prepareSearch(INDEX).setQuery(matchNoneQuery).get();
        assertHitCount(matchNoneResponse, 0L);

        MatchPhraseQueryBuilder matchAllQuery = baseQuery.zeroTermsQuery(ZeroTermsQuery.ALL);
        SearchResponse matchAllResponse = client().prepareSearch(INDEX).setQuery(matchAllQuery).get();
        assertHitCount(matchAllResponse, 2L);
    }

    private List<IndexRequestBuilder> getIndexRequests() {
        List<IndexRequestBuilder> requests = new ArrayList<>();
        requests.add(client().prepareIndex(INDEX).setSource("name", "the beatles"));
        requests.add(client().prepareIndex(INDEX).setSource("name", "led zeppelin"));
        return requests;
    }
}
