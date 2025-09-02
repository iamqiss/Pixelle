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

package org.density.search.msearch;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.density.action.search.MultiSearchRequest;
import org.density.action.search.MultiSearchResponse;
import org.density.common.settings.Settings;
import org.density.core.xcontent.MediaTypeRegistry;
import org.density.index.query.QueryBuilders;
import org.density.test.ParameterizedStaticSettingsDensityIntegTestCase;

import java.util.Arrays;
import java.util.Collection;

import static org.density.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.density.test.hamcrest.DensityAssertions.assertFirstHit;
import static org.density.test.hamcrest.DensityAssertions.assertHitCount;
import static org.density.test.hamcrest.DensityAssertions.assertNoFailures;
import static org.density.test.hamcrest.DensityAssertions.hasId;
import static org.hamcrest.Matchers.equalTo;

public class MultiSearchIT extends ParameterizedStaticSettingsDensityIntegTestCase {

    public MultiSearchIT(Settings staticSettings) {
        super(staticSettings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), false).build() },
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), true).build() }
        );
    }

    public void testSimpleMultiSearch() throws InterruptedException {
        createIndex("test");
        ensureGreen();
        client().prepareIndex("test").setId("1").setSource("field", "xxx").get();
        client().prepareIndex("test").setId("2").setSource("field", "yyy").get();
        refresh();
        indexRandomForConcurrentSearch("test");
        MultiSearchResponse response = client().prepareMultiSearch()
            .add(client().prepareSearch("test").setQuery(QueryBuilders.termQuery("field", "xxx")))
            .add(client().prepareSearch("test").setQuery(QueryBuilders.termQuery("field", "yyy")))
            .add(client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery()))
            .get();

        for (MultiSearchResponse.Item item : response) {
            assertNoFailures(item.getResponse());
        }
        assertThat(response.getResponses().length, equalTo(3));
        assertHitCount(response.getResponses()[0].getResponse(), 1L);
        assertHitCount(response.getResponses()[1].getResponse(), 1L);
        assertHitCount(response.getResponses()[2].getResponse(), 2L);
        assertFirstHit(response.getResponses()[0].getResponse(), hasId("1"));
        assertFirstHit(response.getResponses()[1].getResponse(), hasId("2"));
    }

    public void testSimpleMultiSearchMoreRequests() throws InterruptedException {
        createIndex("test");
        int numDocs = randomIntBetween(0, 16);
        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex("test").setId(Integer.toString(i)).setSource("{}", MediaTypeRegistry.JSON).get();
        }
        refresh();
        indexRandomForConcurrentSearch("test");

        int numSearchRequests = randomIntBetween(1, 64);
        MultiSearchRequest request = new MultiSearchRequest();
        if (randomBoolean()) {
            request.maxConcurrentSearchRequests(randomIntBetween(1, numSearchRequests));
        }
        for (int i = 0; i < numSearchRequests; i++) {
            request.add(client().prepareSearch("test"));
        }

        MultiSearchResponse response = client().multiSearch(request).actionGet();
        assertThat(response.getResponses().length, equalTo(numSearchRequests));
        for (MultiSearchResponse.Item item : response) {
            assertNoFailures(item.getResponse());
            assertHitCount(item.getResponse(), numDocs);
        }
    }

}
