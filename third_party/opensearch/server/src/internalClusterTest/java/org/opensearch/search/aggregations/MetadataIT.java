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

package org.density.search.aggregations;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.density.action.index.IndexRequestBuilder;
import org.density.action.search.SearchResponse;
import org.density.common.settings.Settings;
import org.density.search.aggregations.bucket.terms.Terms;
import org.density.search.aggregations.metrics.Sum;
import org.density.search.aggregations.pipeline.InternalBucketMetricValue;
import org.density.test.ParameterizedStaticSettingsDensityIntegTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.density.common.xcontent.XContentFactory.jsonBuilder;
import static org.density.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_MODE;
import static org.density.search.SearchService.CONCURRENT_SEGMENT_SEARCH_MODE_ALL;
import static org.density.search.SearchService.CONCURRENT_SEGMENT_SEARCH_MODE_AUTO;
import static org.density.search.SearchService.CONCURRENT_SEGMENT_SEARCH_MODE_NONE;
import static org.density.search.aggregations.AggregationBuilders.sum;
import static org.density.search.aggregations.AggregationBuilders.terms;
import static org.density.search.aggregations.PipelineAggregatorBuilders.maxBucket;
import static org.density.test.hamcrest.DensityAssertions.assertAcked;
import static org.density.test.hamcrest.DensityAssertions.assertSearchResponse;

public class MetadataIT extends ParameterizedStaticSettingsDensityIntegTestCase {

    public MetadataIT(Settings staticSettings) {
        super(staticSettings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] {
                Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_MODE.getKey(), CONCURRENT_SEGMENT_SEARCH_MODE_ALL).build() },
            new Object[] {
                Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_MODE.getKey(), CONCURRENT_SEGMENT_SEARCH_MODE_AUTO).build() },
            new Object[] {
                Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_MODE.getKey(), CONCURRENT_SEGMENT_SEARCH_MODE_NONE).build() }
        );
    }

    public void testMetadataSetOnAggregationResult() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("idx").setMapping("name", "type=keyword").get());
        IndexRequestBuilder[] builders = new IndexRequestBuilder[randomInt(30)];
        for (int i = 0; i < builders.length; i++) {
            String name = "name_" + randomIntBetween(1, 10);
            builders[i] = client().prepareIndex("idx")
                .setSource(jsonBuilder().startObject().field("name", name).field("value", randomInt()).endObject());
        }
        indexRandom(true, builders);
        ensureSearchable();

        final Map<String, Object> nestedMetadata = new HashMap<String, Object>() {
            {
                put("nested", "value");
            }
        };

        Map<String, Object> metadata = new HashMap<String, Object>() {
            {
                put("key", "value");
                put("numeric", 1.2);
                put("bool", true);
                put("complex", nestedMetadata);
            }
        };

        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                terms("the_terms").setMetadata(metadata).field("name").subAggregation(sum("the_sum").setMetadata(metadata).field("value"))
            )
            .addAggregation(maxBucket("the_max_bucket", "the_terms>the_sum").setMetadata(metadata))
            .get();

        assertSearchResponse(response);

        Aggregations aggs = response.getAggregations();
        assertNotNull(aggs);

        Terms terms = aggs.get("the_terms");
        assertNotNull(terms);
        assertMetadata(terms.getMetadata());

        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        for (Terms.Bucket bucket : buckets) {
            Aggregations subAggs = bucket.getAggregations();
            assertNotNull(subAggs);

            Sum sum = subAggs.get("the_sum");
            assertNotNull(sum);
            assertMetadata(sum.getMetadata());
        }

        InternalBucketMetricValue maxBucket = aggs.get("the_max_bucket");
        assertNotNull(maxBucket);
        assertMetadata(maxBucket.getMetadata());
    }

    private void assertMetadata(Map<String, Object> returnedMetadata) {
        assertNotNull(returnedMetadata);
        assertEquals(4, returnedMetadata.size());
        assertEquals("value", returnedMetadata.get("key"));
        assertEquals(1.2, returnedMetadata.get("numeric"));
        assertEquals(true, returnedMetadata.get("bool"));

        Object nestedObject = returnedMetadata.get("complex");
        assertNotNull(nestedObject);

        Map<String, Object> nestedMap = (Map<String, Object>) nestedObject;
        assertEquals("value", nestedMap.get("nested"));
    }
}
