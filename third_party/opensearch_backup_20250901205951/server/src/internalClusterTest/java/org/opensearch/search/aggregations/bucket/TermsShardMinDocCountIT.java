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
import org.density.core.xcontent.MediaTypeRegistry;
import org.density.index.query.QueryBuilders;
import org.density.search.aggregations.BucketOrder;
import org.density.search.aggregations.bucket.filter.InternalFilter;
import org.density.search.aggregations.bucket.terms.SignificantTerms;
import org.density.search.aggregations.bucket.terms.SignificantTermsAggregatorFactory;
import org.density.search.aggregations.bucket.terms.Terms;
import org.density.test.ParameterizedStaticSettingsDensityIntegTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.density.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.density.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.density.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.density.search.aggregations.AggregationBuilders.filter;
import static org.density.search.aggregations.AggregationBuilders.significantTerms;
import static org.density.search.aggregations.AggregationBuilders.terms;
import static org.density.test.hamcrest.DensityAssertions.assertAcked;
import static org.density.test.hamcrest.DensityAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;

public class TermsShardMinDocCountIT extends ParameterizedStaticSettingsDensityIntegTestCase {

    private static final String index = "someindex";

    public TermsShardMinDocCountIT(Settings staticSettings) {
        super(staticSettings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), false).build() },
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), true).build() }
        );
    }

    private static String randomExecutionHint() {
        return randomBoolean() ? null : randomFrom(SignificantTermsAggregatorFactory.ExecutionMode.values()).toString();
    }

    // see https://github.com/elastic/elasticsearch/issues/5998
    public void testShardMinDocCountSignificantTermsTest() throws Exception {
        assumeFalse(
            "For concurrent segment search shard_min_doc_count is not enforced at the slice level. See https://github.com/density-project/Density/issues/11847",
            internalCluster().clusterService().getClusterSettings().get(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING)
        );
        String textMappings;
        if (randomBoolean()) {
            textMappings = "type=long";
        } else {
            textMappings = "type=text,fielddata=true";
        }
        assertAcked(
            prepareCreate(index).setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0))
                .setMapping("text", textMappings)
        );
        List<IndexRequestBuilder> indexBuilders = new ArrayList<>();

        addTermsDocs("1", 1, 0, indexBuilders);// high score but low doc freq
        addTermsDocs("2", 1, 0, indexBuilders);
        addTermsDocs("3", 1, 0, indexBuilders);
        addTermsDocs("4", 1, 0, indexBuilders);
        addTermsDocs("5", 3, 1, indexBuilders);// low score but high doc freq
        addTermsDocs("6", 3, 1, indexBuilders);
        addTermsDocs("7", 0, 3, indexBuilders);// make sure the terms all get score > 0 except for this one
        indexRandom(true, false, indexBuilders);

        // first, check that indeed when not setting the shardMinDocCount parameter 0 terms are returned
        SearchResponse response = client().prepareSearch(index)
            .addAggregation(
                (filter("inclass", QueryBuilders.termQuery("class", true))).subAggregation(
                    significantTerms("mySignificantTerms").field("text")
                        .minDocCount(2)
                        .size(2)
                        .shardSize(2)
                        .executionHint(randomExecutionHint())
                )
            )
            .get();
        assertSearchResponse(response);
        InternalFilter filteredBucket = response.getAggregations().get("inclass");
        SignificantTerms sigterms = filteredBucket.getAggregations().get("mySignificantTerms");
        assertThat(sigterms.getBuckets().size(), equalTo(0));

        response = client().prepareSearch(index)
            .addAggregation(
                (filter("inclass", QueryBuilders.termQuery("class", true))).subAggregation(
                    significantTerms("mySignificantTerms").field("text")
                        .minDocCount(2)
                        .shardSize(2)
                        .shardMinDocCount(2)
                        .size(2)
                        .executionHint(randomExecutionHint())
                )
            )
            .get();
        assertSearchResponse(response);
        filteredBucket = response.getAggregations().get("inclass");
        sigterms = filteredBucket.getAggregations().get("mySignificantTerms");
        assertThat(sigterms.getBuckets().size(), equalTo(2));
    }

    private void addTermsDocs(String term, int numInClass, int numNotInClass, List<IndexRequestBuilder> builders) {
        String sourceClass = "{\"text\": \"" + term + "\", \"class\":" + "true" + "}";
        String sourceNotClass = "{\"text\": \"" + term + "\", \"class\":" + "false" + "}";
        for (int i = 0; i < numInClass; i++) {
            builders.add(client().prepareIndex(index).setSource(sourceClass, MediaTypeRegistry.JSON));
        }
        for (int i = 0; i < numNotInClass; i++) {
            builders.add(client().prepareIndex(index).setSource(sourceNotClass, MediaTypeRegistry.JSON));
        }
    }

    // see https://github.com/elastic/elasticsearch/issues/5998
    public void testShardMinDocCountTermsTest() throws Exception {
        assumeFalse(
            "For concurrent segment search shard_min_doc_count is not enforced at the slice level. See https://github.com/density-project/Density/issues/11847",
            internalCluster().clusterService().getClusterSettings().get(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING)
        );
        final String[] termTypes = { "text", "long", "integer", "float", "double" };
        String termtype = termTypes[randomInt(termTypes.length - 1)];
        String termMappings = "type=" + termtype;
        if (termtype.equals("text")) {
            termMappings += ",fielddata=true";
        }
        assertAcked(
            prepareCreate(index).setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0))
                .setMapping("text", termMappings)
        );
        List<IndexRequestBuilder> indexBuilders = new ArrayList<>();

        addTermsDocs("1", 1, indexBuilders);// low doc freq but high score
        addTermsDocs("2", 1, indexBuilders);
        addTermsDocs("3", 1, indexBuilders);
        addTermsDocs("4", 1, indexBuilders);
        addTermsDocs("5", 3, indexBuilders);// low score but high doc freq
        addTermsDocs("6", 3, indexBuilders);
        indexRandom(true, false, indexBuilders);

        // first, check that indeed when not setting the shardMinDocCount parameter 0 terms are returned
        SearchResponse response = client().prepareSearch(index)
            .addAggregation(
                terms("myTerms").field("text")
                    .minDocCount(2)
                    .size(2)
                    .shardSize(2)
                    .executionHint(randomExecutionHint())
                    .order(BucketOrder.key(true))
            )
            .get();
        assertSearchResponse(response);
        Terms terms = response.getAggregations().get("myTerms");
        assertThat(terms.getBuckets().size(), equalTo(0));

        response = client().prepareSearch(index)
            .addAggregation(
                terms("myTerms").field("text")
                    .minDocCount(2)
                    .shardMinDocCount(2)
                    .size(2)
                    .shardSize(2)
                    .executionHint(randomExecutionHint())
                    .order(BucketOrder.key(true))
            )
            .get();
        assertSearchResponse(response);
        terms = response.getAggregations().get("myTerms");
        assertThat(terms.getBuckets().size(), equalTo(2));

    }

    private static void addTermsDocs(String term, int numDocs, List<IndexRequestBuilder> builders) {
        String sourceClass = "{\"text\": \"" + term + "\"}";
        for (int i = 0; i < numDocs; i++) {
            builders.add(client().prepareIndex(index).setSource(sourceClass, MediaTypeRegistry.JSON));
        }
    }
}
