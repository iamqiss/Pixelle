/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.search.aggregations.bucket;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.density.action.search.SearchResponse;
import org.density.cluster.health.ClusterHealthStatus;
import org.density.cluster.metadata.IndexMetadata;
import org.density.common.settings.Settings;
import org.density.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.density.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.density.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.density.search.aggregations.metrics.MaxAggregationBuilder;
import org.density.test.DensityIntegTestCase;
import org.density.test.ParameterizedStaticSettingsDensityIntegTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.density.indices.IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING;
import static org.density.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.density.test.hamcrest.DensityAssertions.assertAcked;
import static org.density.test.hamcrest.DensityAssertions.assertSearchResponse;

@DensityIntegTestCase.SuiteScopeTestCase
public class CompositeAggIT extends ParameterizedStaticSettingsDensityIntegTestCase {

    public CompositeAggIT(Settings staticSettings) {
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
    public void setupSuiteScopeCluster() throws Exception {
        assertAcked(
            prepareCreate(
                "idx",
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(INDEX_CACHE_REQUEST_ENABLED_SETTING.getKey(), false)
            ).setMapping("type", "type=keyword", "num", "type=integer", "score", "type=integer")
        );
        waitForRelocation(ClusterHealthStatus.GREEN);

        indexRandom(
            true,
            client().prepareIndex("idx").setId("1").setSource("type", "type1", "num", "1", "score", "5"),
            client().prepareIndex("idx").setId("1").setSource("type", "type2", "num", "11", "score", "50"),
            client().prepareIndex("idx").setId("1").setSource("type", "type1", "num", "1", "score", "2"),
            client().prepareIndex("idx").setId("1").setSource("type", "type2", "num", "12", "score", "20"),
            client().prepareIndex("idx").setId("1").setSource("type", "type1", "num", "3", "score", "10"),
            client().prepareIndex("idx").setId("1").setSource("type", "type2", "num", "13", "score", "15"),
            client().prepareIndex("idx").setId("1").setSource("type", "type1", "num", "3", "score", "1"),
            client().prepareIndex("idx").setId("1").setSource("type", "type2", "num", "13", "score", "100")
        );

        waitForRelocation(ClusterHealthStatus.GREEN);
        refresh();
    }

    public void testCompositeAggWithNoSubAgg() {
        SearchResponse rsp = client().prepareSearch("idx")
            .addAggregation(new CompositeAggregationBuilder("my_composite", getTestValueSources()))
            .get();
        assertSearchResponse(rsp);
    }

    public void testCompositeAggWithSubAgg() {
        SearchResponse rsp = client().prepareSearch("idx")
            .addAggregation(
                new CompositeAggregationBuilder("my_composite", getTestValueSources()).subAggregation(
                    new MaxAggregationBuilder("max").field("score")
                )
            )
            .get();
        assertSearchResponse(rsp);
    }

    private List<CompositeValuesSourceBuilder<?>> getTestValueSources() {
        final List<CompositeValuesSourceBuilder<?>> sources = new ArrayList<>();
        sources.add(new TermsValuesSourceBuilder("keyword_vs").field("type"));
        sources.add(new TermsValuesSourceBuilder("num_vs").field("num"));
        return sources;
    }
}
