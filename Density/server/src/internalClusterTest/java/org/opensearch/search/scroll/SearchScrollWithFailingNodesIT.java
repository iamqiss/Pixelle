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

package org.density.search.scroll;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.density.action.index.IndexRequestBuilder;
import org.density.action.search.SearchResponse;
import org.density.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.density.common.settings.Settings;
import org.density.common.unit.TimeValue;
import org.density.test.DensityIntegTestCase;
import org.density.test.ParameterizedStaticSettingsDensityIntegTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.density.common.xcontent.XContentFactory.jsonBuilder;
import static org.density.index.query.QueryBuilders.matchAllQuery;
import static org.density.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.density.test.hamcrest.DensityAssertions.assertAcked;
import static org.density.test.hamcrest.DensityAssertions.assertAllSuccessful;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

@DensityIntegTestCase.ClusterScope(scope = DensityIntegTestCase.Scope.TEST, numDataNodes = 2, numClientNodes = 0)
public class SearchScrollWithFailingNodesIT extends ParameterizedStaticSettingsDensityIntegTestCase {
    public SearchScrollWithFailingNodesIT(Settings settings) {
        super(settings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), false).build() },
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), true).build() }
        );
    }

    @Override
    protected int numberOfShards() {
        return 2;
    }

    @Override
    protected int numberOfReplicas() {
        return 0;
    }

    public void testScanScrollWithShardExceptions() throws Exception {
        assertAcked(
            prepareCreate("test")
                // Enforces that only one shard can only be allocated to a single node
                .setSettings(
                    Settings.builder()
                        .put(indexSettings())
                        .put(ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), 1)
                )
        );

        List<IndexRequestBuilder> writes = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            writes.add(client().prepareIndex("test").setSource(jsonBuilder().startObject().field("field", i).endObject()));
        }
        indexRandom(false, writes);
        refresh();

        SearchResponse searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .setSize(10)
            .setScroll(TimeValue.timeValueMinutes(1))
            .get();
        assertAllSuccessful(searchResponse);
        long numHits = 0;
        do {
            numHits += searchResponse.getHits().getHits().length;
            searchResponse = client().prepareSearchScroll(searchResponse.getScrollId()).setScroll(TimeValue.timeValueMinutes(1)).get();
            assertAllSuccessful(searchResponse);
        } while (searchResponse.getHits().getHits().length > 0);
        assertThat(numHits, equalTo(100L));
        clearScroll("_all");

        internalCluster().stopRandomDataNode();

        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setSize(10).setScroll(TimeValue.timeValueMinutes(1)).get();
        assertThat(searchResponse.getSuccessfulShards(), lessThan(searchResponse.getTotalShards()));
        numHits = 0;
        int numberOfSuccessfulShards = searchResponse.getSuccessfulShards();
        do {
            numHits += searchResponse.getHits().getHits().length;
            searchResponse = client().prepareSearchScroll(searchResponse.getScrollId()).setScroll(TimeValue.timeValueMinutes(1)).get();
            assertThat(searchResponse.getSuccessfulShards(), equalTo(numberOfSuccessfulShards));
        } while (searchResponse.getHits().getHits().length > 0);
        assertThat(numHits, greaterThan(0L));

        clearScroll(searchResponse.getScrollId());
    }

}
