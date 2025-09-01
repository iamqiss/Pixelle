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

package org.density.search.aggregations.bucket;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.density.action.index.IndexRequestBuilder;
import org.density.action.search.SearchResponse;
import org.density.common.settings.Settings;
import org.density.search.aggregations.Aggregator.SubAggCollectionMode;
import org.density.search.aggregations.bucket.terms.Terms;
import org.density.test.DensityIntegTestCase;
import org.density.test.ParameterizedDynamicSettingsDensityIntegTestCase;

import java.util.Arrays;
import java.util.Collection;

import static org.density.common.xcontent.XContentFactory.jsonBuilder;
import static org.density.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.density.search.aggregations.AggregationBuilders.terms;
import static org.density.test.hamcrest.DensityAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.IsNull.notNullValue;

@DensityIntegTestCase.SuiteScopeTestCase
public class BooleanTermsIT extends ParameterizedDynamicSettingsDensityIntegTestCase {

    private static final String SINGLE_VALUED_FIELD_NAME = "b_value";
    private static final String MULTI_VALUED_FIELD_NAME = "b_values";

    static int numSingleTrues, numSingleFalses, numMultiTrues, numMultiFalses;

    public BooleanTermsIT(Settings dynamicSettings) {
        super(dynamicSettings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), false).build() },
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), true).build() }
        );
    }

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        createIndex("idx");
        createIndex("idx_unmapped");
        ensureSearchable();
        final int numDocs = randomInt(5);
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < builders.length; i++) {
            final boolean singleValue = randomBoolean();
            if (singleValue) {
                numSingleTrues++;
            } else {
                numSingleFalses++;
            }
            final boolean[] multiValue;
            switch (randomInt(3)) {
                case 0:
                    multiValue = new boolean[0];
                    break;
                case 1:
                    numMultiFalses++;
                    multiValue = new boolean[] { false };
                    break;
                case 2:
                    numMultiTrues++;
                    multiValue = new boolean[] { true };
                    break;
                case 3:
                    numMultiFalses++;
                    numMultiTrues++;
                    multiValue = new boolean[] { false, true };
                    break;
                default:
                    throw new AssertionError();
            }
            builders[i] = client().prepareIndex("idx")
                .setSource(
                    jsonBuilder().startObject()
                        .field(SINGLE_VALUED_FIELD_NAME, singleValue)
                        .array(MULTI_VALUED_FIELD_NAME, multiValue)
                        .endObject()
                );
        }
        indexRandom(true, builders);
    }

    public void testSingleValueField() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(terms("terms").field(SINGLE_VALUED_FIELD_NAME).collectMode(randomFrom(SubAggCollectionMode.values())))
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        final int bucketCount = numSingleFalses > 0 && numSingleTrues > 0 ? 2 : numSingleFalses + numSingleTrues > 0 ? 1 : 0;
        assertThat(terms.getBuckets().size(), equalTo(bucketCount));

        Terms.Bucket bucket = terms.getBucketByKey("false");
        if (numSingleFalses == 0) {
            assertNull(bucket);
        } else {
            assertNotNull(bucket);
            assertEquals(numSingleFalses, bucket.getDocCount());
            assertEquals("false", bucket.getKeyAsString());
        }

        bucket = terms.getBucketByKey("true");
        if (numSingleTrues == 0) {
            assertNull(bucket);
        } else {
            assertNotNull(bucket);
            assertEquals(numSingleTrues, bucket.getDocCount());
            assertEquals("true", bucket.getKeyAsString());
        }
    }

    public void testMultiValueField() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(terms("terms").field(MULTI_VALUED_FIELD_NAME).collectMode(randomFrom(SubAggCollectionMode.values())))
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        final int bucketCount = numMultiFalses > 0 && numMultiTrues > 0 ? 2 : numMultiFalses + numMultiTrues > 0 ? 1 : 0;
        assertThat(terms.getBuckets().size(), equalTo(bucketCount));

        Terms.Bucket bucket = terms.getBucketByKey("false");
        if (numMultiFalses == 0) {
            assertNull(bucket);
        } else {
            assertNotNull(bucket);
            assertEquals(numMultiFalses, bucket.getDocCount());
            assertEquals("false", bucket.getKeyAsString());
        }

        bucket = terms.getBucketByKey("true");
        if (numMultiTrues == 0) {
            assertNull(bucket);
        } else {
            assertNotNull(bucket);
            assertEquals(numMultiTrues, bucket.getDocCount());
            assertEquals("true", bucket.getKeyAsString());
        }
    }

    public void testUnmapped() throws Exception {
        SearchResponse response = client().prepareSearch("idx_unmapped")
            .addAggregation(
                terms("terms").field(SINGLE_VALUED_FIELD_NAME).size(between(1, 5)).collectMode(randomFrom(SubAggCollectionMode.values()))
            )
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(0));
    }
}
