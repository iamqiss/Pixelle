/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.search.aggregations.bucket;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.density.action.admin.indices.segments.IndicesSegmentResponse;
import org.density.action.search.SearchResponse;
import org.density.cluster.metadata.IndexMetadata;
import org.density.common.settings.Settings;
import org.density.search.aggregations.bucket.terms.Terms;
import org.density.test.DensityIntegTestCase;
import org.density.test.ParameterizedStaticSettingsDensityIntegTestCase;

import java.util.Arrays;
import java.util.Collection;

import static org.density.common.xcontent.XContentFactory.jsonBuilder;
import static org.density.index.IndexSettings.MINIMUM_REFRESH_INTERVAL;
import static org.density.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.density.search.aggregations.AggregationBuilders.terms;
import static org.density.test.DensityIntegTestCase.Scope.TEST;
import static org.density.test.hamcrest.DensityAssertions.assertAcked;

@DensityIntegTestCase.ClusterScope(scope = TEST, numClientNodes = 0, maxNumDataNodes = 1, supportsDedicatedMasters = false)
public class TermsFixedDocCountErrorIT extends ParameterizedStaticSettingsDensityIntegTestCase {

    private static final String STRING_FIELD_NAME = "s_value";

    public TermsFixedDocCountErrorIT(Settings staticSettings) {
        super(staticSettings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), true).build() },
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), false).build() }
        );
    }

    public void testSimpleAggErrorMultiShard() throws Exception {
        // size = 1, shard_size = 2
        // Shard_1 [A, A, A, A, B, B, C, C, D, D] -> Buckets {"A" : 4, "B" : 2}
        // Shard_2 [A, B, B, B, C, C, C, D, D, D] -> Buckets {"B" : 3, "C" : 3}
        // coordinator -> Buckets {"B" : 5, "A" : 4}
        // Agg error is 4, from (shard_size)th bucket on each shard
        // Bucket "A" error is 2, from (shard_size)th bucket on shard_2
        // Bucket "B" error is 0, it's present on both shards

        // size = 1 shard_size = 1 slice_size = 1
        // non-cs / cs
        // Shard_1 [A, B, C]
        // Shard_2 [B, C, D]
        // cs
        // Shard_1 slice_1 [A, B, C] -> {a : 1} -> {a : 1 -- error: 1}
        // slice_2 [B, C, D] -> {b : 1}
        // Coordinator should return the same doc count error in both cases

        assertAcked(
            prepareCreate("idx_mshard_1").setMapping(STRING_FIELD_NAME, "type=keyword")
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put("index.refresh_interval", MINIMUM_REFRESH_INTERVAL)
                )
        );
        client().prepareIndex("idx_mshard_1").setSource(jsonBuilder().startObject().field(STRING_FIELD_NAME, "A").endObject()).get();
        client().prepareIndex("idx_mshard_1").setSource(jsonBuilder().startObject().field(STRING_FIELD_NAME, "A").endObject()).get();
        client().prepareIndex("idx_mshard_1").setSource(jsonBuilder().startObject().field(STRING_FIELD_NAME, "A").endObject()).get();
        client().prepareIndex("idx_mshard_1").setSource(jsonBuilder().startObject().field(STRING_FIELD_NAME, "A").endObject()).get();
        client().prepareIndex("idx_mshard_1").setSource(jsonBuilder().startObject().field(STRING_FIELD_NAME, "B").endObject()).get();
        client().prepareIndex("idx_mshard_1").setSource(jsonBuilder().startObject().field(STRING_FIELD_NAME, "B").endObject()).get();
        client().prepareIndex("idx_mshard_1").setSource(jsonBuilder().startObject().field(STRING_FIELD_NAME, "C").endObject()).get();
        refresh("idx_mshard_1");

        IndicesSegmentResponse segmentResponse = client().admin().indices().prepareSegments("idx_mshard_1").get();
        assertEquals(1, segmentResponse.getIndices().get("idx_mshard_1").getShards().get(0).getShards()[0].getSegments().size());

        assertAcked(
            prepareCreate("idx_mshard_2").setMapping(STRING_FIELD_NAME, "type=keyword")
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put("index.refresh_interval", MINIMUM_REFRESH_INTERVAL)
                )
        );
        client().prepareIndex("idx_mshard_2").setSource(jsonBuilder().startObject().field(STRING_FIELD_NAME, "A").endObject()).get();
        client().prepareIndex("idx_mshard_2").setSource(jsonBuilder().startObject().field(STRING_FIELD_NAME, "B").endObject()).get();
        client().prepareIndex("idx_mshard_2").setSource(jsonBuilder().startObject().field(STRING_FIELD_NAME, "B").endObject()).get();
        client().prepareIndex("idx_mshard_2").setSource(jsonBuilder().startObject().field(STRING_FIELD_NAME, "B").endObject()).get();
        client().prepareIndex("idx_mshard_2").setSource(jsonBuilder().startObject().field(STRING_FIELD_NAME, "C").endObject()).get();
        client().prepareIndex("idx_mshard_2").setSource(jsonBuilder().startObject().field(STRING_FIELD_NAME, "C").endObject()).get();
        refresh("idx_mshard_2");

        segmentResponse = client().admin().indices().prepareSegments("idx_mshard_2").get();
        assertEquals(1, segmentResponse.getIndices().get("idx_mshard_2").getShards().get(0).getShards()[0].getSegments().size());

        SearchResponse response = client().prepareSearch("idx_mshard_2", "idx_mshard_1")
            .setSize(0)
            .addAggregation(terms("terms").field(STRING_FIELD_NAME).showTermDocCountError(true).size(2).shardSize(2))
            .get();

        Terms terms = response.getAggregations().get("terms");
        assertEquals(2, terms.getBuckets().size());
        assertEquals(4, terms.getDocCountError());

        Terms.Bucket bucket = terms.getBuckets().get(0); // Bucket "B"
        assertEquals("B", bucket.getKey().toString());
        assertEquals(5, bucket.getDocCount());
        assertEquals(0, bucket.getDocCountError());

        bucket = terms.getBuckets().get(1); // Bucket "A"
        assertEquals("A", bucket.getKey().toString());
        assertEquals(4, bucket.getDocCount());
        assertEquals(2, bucket.getDocCountError());
    }

    public void testSimpleAggErrorSingleShard() throws Exception {
        assertAcked(
            prepareCreate("idx_shard_error").setMapping(STRING_FIELD_NAME, "type=keyword")
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put("index.refresh_interval", MINIMUM_REFRESH_INTERVAL)
                )
        );
        client().prepareIndex("idx_shard_error").setSource(jsonBuilder().startObject().field(STRING_FIELD_NAME, "A").endObject()).get();
        client().prepareIndex("idx_shard_error").setSource(jsonBuilder().startObject().field(STRING_FIELD_NAME, "A").endObject()).get();
        client().prepareIndex("idx_shard_error").setSource(jsonBuilder().startObject().field(STRING_FIELD_NAME, "A").endObject()).get();
        client().prepareIndex("idx_shard_error").setSource(jsonBuilder().startObject().field(STRING_FIELD_NAME, "A").endObject()).get();
        client().prepareIndex("idx_shard_error").setSource(jsonBuilder().startObject().field(STRING_FIELD_NAME, "A").endObject()).get();
        client().prepareIndex("idx_shard_error").setSource(jsonBuilder().startObject().field(STRING_FIELD_NAME, "A").endObject()).get();
        client().prepareIndex("idx_shard_error").setSource(jsonBuilder().startObject().field(STRING_FIELD_NAME, "B").endObject()).get();
        client().prepareIndex("idx_shard_error").setSource(jsonBuilder().startObject().field(STRING_FIELD_NAME, "B").endObject()).get();
        refresh("idx_shard_error");

        SearchResponse response = client().prepareSearch("idx_shard_error")
            .setSize(0)
            .addAggregation(terms("terms").field(STRING_FIELD_NAME).showTermDocCountError(true).size(1).shardSize(2))
            .get();

        Terms terms = response.getAggregations().get("terms");
        assertEquals(1, terms.getBuckets().size());
        assertEquals(0, terms.getDocCountError());

        Terms.Bucket bucket = terms.getBuckets().get(0);
        assertEquals("A", bucket.getKey().toString());
        assertEquals(6, bucket.getDocCount());
        assertEquals(0, bucket.getDocCountError());
    }

    public void testSliceLevelDocCountErrorSingleShard() throws Exception {
        assumeTrue(
            "Slice level error is not relevant to non-concurrent search cases",
            internalCluster().clusterService().getClusterSettings().get(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING)
        );

        // Slices are created by sorting segments by doc count in descending order then distributing in round robin fashion.
        // Creates 2 segments (and therefore 2 slices since slice_count = 2) as follows:
        // 1. [A, A, A, B, B, C]
        // 2. [A, B, B, B, C, C]
        // Thus we expect the doc count error for A to be 2 as the nth largest bucket on slice 2 has size 2

        assertAcked(
            prepareCreate("idx_slice_error").setMapping(STRING_FIELD_NAME, "type=keyword")
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put("index.refresh_interval", MINIMUM_REFRESH_INTERVAL)
                )
        );
        client().prepareIndex("idx_slice_error").setSource(jsonBuilder().startObject().field(STRING_FIELD_NAME, "A").endObject()).get();
        client().prepareIndex("idx_slice_error").setSource(jsonBuilder().startObject().field(STRING_FIELD_NAME, "A").endObject()).get();
        client().prepareIndex("idx_slice_error").setSource(jsonBuilder().startObject().field(STRING_FIELD_NAME, "A").endObject()).get();
        client().prepareIndex("idx_slice_error").setSource(jsonBuilder().startObject().field(STRING_FIELD_NAME, "B").endObject()).get();
        client().prepareIndex("idx_slice_error").setSource(jsonBuilder().startObject().field(STRING_FIELD_NAME, "B").endObject()).get();
        client().prepareIndex("idx_slice_error").setSource(jsonBuilder().startObject().field(STRING_FIELD_NAME, "C").endObject()).get();
        refresh("idx_slice_error");

        client().prepareIndex("idx_slice_error").setSource(jsonBuilder().startObject().field(STRING_FIELD_NAME, "A").endObject()).get();
        client().prepareIndex("idx_slice_error").setSource(jsonBuilder().startObject().field(STRING_FIELD_NAME, "B").endObject()).get();
        client().prepareIndex("idx_slice_error").setSource(jsonBuilder().startObject().field(STRING_FIELD_NAME, "B").endObject()).get();
        client().prepareIndex("idx_slice_error").setSource(jsonBuilder().startObject().field(STRING_FIELD_NAME, "B").endObject()).get();
        client().prepareIndex("idx_slice_error").setSource(jsonBuilder().startObject().field(STRING_FIELD_NAME, "C").endObject()).get();
        client().prepareIndex("idx_slice_error").setSource(jsonBuilder().startObject().field(STRING_FIELD_NAME, "C").endObject()).get();
        refresh("idx_slice_error");

        IndicesSegmentResponse segmentResponse = client().admin().indices().prepareSegments("idx_slice_error").get();
        assertEquals(2, segmentResponse.getIndices().get("idx_slice_error").getShards().get(0).getShards()[0].getSegments().size());

        // Confirm that there is no error when shard_size == slice_size > cardinality
        SearchResponse response = client().prepareSearch("idx_slice_error")
            .setSize(0)
            .addAggregation(terms("terms").field(STRING_FIELD_NAME).showTermDocCountError(true).size(1).shardSize(4))
            .get();

        Terms terms = response.getAggregations().get("terms");
        assertEquals(1, terms.getBuckets().size());
        assertEquals(0, terms.getDocCountError());

        Terms.Bucket bucket = terms.getBuckets().get(0); // Bucket "B"
        assertEquals("B", bucket.getKey().toString());
        assertEquals(5, bucket.getDocCount());
        assertEquals(0, bucket.getDocCountError());

        response = client().prepareSearch("idx_slice_error")
            .setSize(0)
            .addAggregation(terms("terms").field(STRING_FIELD_NAME).showTermDocCountError(true).size(2).shardSize(2))
            .get();

        terms = response.getAggregations().get("terms");
        assertEquals(2, terms.getBuckets().size());
        assertEquals(4, terms.getDocCountError());

        bucket = terms.getBuckets().get(0); // Bucket "B"
        assertEquals("B", bucket.getKey().toString());
        assertEquals(5, bucket.getDocCount());
        assertEquals(0, bucket.getDocCountError());

        bucket = terms.getBuckets().get(1); // Bucket "A"
        assertEquals("A", bucket.getKey().toString());
        assertEquals(3, bucket.getDocCount());
        assertEquals(2, bucket.getDocCountError());
    }

    public void testSliceLevelDocCountErrorMultiShard() throws Exception {
        assumeTrue(
            "Slice level error is not relevant to non-concurrent search cases",
            internalCluster().clusterService().getClusterSettings().get(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING)
        );

        // Size = 2, shard_size = 2
        // Shard_1 [A, A, A, A, B, B, C, C]
        // slice_1 [A, A, A, B, B, C] {"A" : 3, "B" : 2}
        // slice_2 [A, C] {"A" : 1, "C" : 1}
        // Shard_1 buckets: {"A" : 4 - error: 0, "B" : 2 - error: 1}
        // Shard_2 [A, A, B, B, B, C, C, C]
        // slice_1 [A, B, B, B, C, C] {"B" : 3, "C" : 2}
        // slice_2 [A, C] {"A" : 1, "C" : 1}
        // Shard_2 buckets: {"B" : 3 - error: 1, "C" : 3 - error: 0}
        // Overall
        // {"B" : 5 - error: 2, "A" : 4 - error: 3} Agg error: 6

        assertAcked(
            prepareCreate("idx_mshard_1").setMapping(STRING_FIELD_NAME, "type=keyword")
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put("index.refresh_interval", MINIMUM_REFRESH_INTERVAL)
                )
        );
        client().prepareIndex("idx_mshard_1").setSource(jsonBuilder().startObject().field(STRING_FIELD_NAME, "A").endObject()).get();
        client().prepareIndex("idx_mshard_1").setSource(jsonBuilder().startObject().field(STRING_FIELD_NAME, "A").endObject()).get();
        client().prepareIndex("idx_mshard_1").setSource(jsonBuilder().startObject().field(STRING_FIELD_NAME, "A").endObject()).get();
        client().prepareIndex("idx_mshard_1").setSource(jsonBuilder().startObject().field(STRING_FIELD_NAME, "B").endObject()).get();
        client().prepareIndex("idx_mshard_1").setSource(jsonBuilder().startObject().field(STRING_FIELD_NAME, "B").endObject()).get();
        client().prepareIndex("idx_mshard_1").setSource(jsonBuilder().startObject().field(STRING_FIELD_NAME, "C").endObject()).get();
        refresh("idx_mshard_1");

        client().prepareIndex("idx_mshard_1").setSource(jsonBuilder().startObject().field(STRING_FIELD_NAME, "A").endObject()).get();
        client().prepareIndex("idx_mshard_1").setSource(jsonBuilder().startObject().field(STRING_FIELD_NAME, "C").endObject()).get();
        refresh("idx_mshard_1");

        IndicesSegmentResponse segmentResponse = client().admin().indices().prepareSegments("idx_mshard_1").get();
        assertEquals(2, segmentResponse.getIndices().get("idx_mshard_1").getShards().get(0).getShards()[0].getSegments().size());

        SearchResponse response = client().prepareSearch("idx_mshard_1")
            .setSize(0)
            .addAggregation(terms("terms").field(STRING_FIELD_NAME).showTermDocCountError(true).size(2).shardSize(2))
            .get();

        Terms terms = response.getAggregations().get("terms");
        assertEquals(2, terms.getBuckets().size());
        assertEquals(3, terms.getDocCountError());

        Terms.Bucket bucket = terms.getBuckets().get(0);
        assertEquals("A", bucket.getKey().toString());
        assertEquals(4, bucket.getDocCount());
        assertEquals(0, bucket.getDocCountError());

        bucket = terms.getBuckets().get(1);
        assertEquals("B", bucket.getKey().toString());
        assertEquals(2, bucket.getDocCount());
        assertEquals(1, bucket.getDocCountError());

        assertAcked(
            prepareCreate("idx_mshard_2").setMapping(STRING_FIELD_NAME, "type=keyword")
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put("index.refresh_interval", MINIMUM_REFRESH_INTERVAL)
                )
        );
        client().prepareIndex("idx_mshard_2").setSource(jsonBuilder().startObject().field(STRING_FIELD_NAME, "A").endObject()).get();
        client().prepareIndex("idx_mshard_2").setSource(jsonBuilder().startObject().field(STRING_FIELD_NAME, "B").endObject()).get();
        client().prepareIndex("idx_mshard_2").setSource(jsonBuilder().startObject().field(STRING_FIELD_NAME, "B").endObject()).get();
        client().prepareIndex("idx_mshard_2").setSource(jsonBuilder().startObject().field(STRING_FIELD_NAME, "B").endObject()).get();
        client().prepareIndex("idx_mshard_2").setSource(jsonBuilder().startObject().field(STRING_FIELD_NAME, "C").endObject()).get();
        client().prepareIndex("idx_mshard_2").setSource(jsonBuilder().startObject().field(STRING_FIELD_NAME, "C").endObject()).get();
        refresh("idx_mshard_2");

        client().prepareIndex("idx_mshard_2").setSource(jsonBuilder().startObject().field(STRING_FIELD_NAME, "A").endObject()).get();
        client().prepareIndex("idx_mshard_2").setSource(jsonBuilder().startObject().field(STRING_FIELD_NAME, "C").endObject()).get();
        refresh("idx_mshard_2");

        segmentResponse = client().admin().indices().prepareSegments("idx_mshard_2").get();
        assertEquals(2, segmentResponse.getIndices().get("idx_mshard_2").getShards().get(0).getShards()[0].getSegments().size());

        response = client().prepareSearch("idx_mshard_2")
            .setSize(0)
            .addAggregation(terms("terms").field(STRING_FIELD_NAME).showTermDocCountError(true).size(2).shardSize(2))
            .get();

        terms = response.getAggregations().get("terms");
        assertEquals(2, terms.getBuckets().size());
        assertEquals(3, terms.getDocCountError());

        bucket = terms.getBuckets().get(0);
        assertEquals("B", bucket.getKey().toString());
        assertEquals(3, bucket.getDocCount());
        assertEquals(1, bucket.getDocCountError());

        bucket = terms.getBuckets().get(1);
        assertEquals("C", bucket.getKey().toString());
        assertEquals(3, bucket.getDocCount());
        assertEquals(0, bucket.getDocCountError());

        response = client().prepareSearch("idx_mshard_2", "idx_mshard_1")
            .setSize(0)
            .addAggregation(terms("terms").field(STRING_FIELD_NAME).showTermDocCountError(true).size(2).shardSize(2))
            .get();

        terms = response.getAggregations().get("terms");
        assertEquals(2, terms.getBuckets().size());
        assertEquals(6, terms.getDocCountError());

        bucket = terms.getBuckets().get(0);
        assertEquals("B", bucket.getKey().toString());
        assertEquals(5, bucket.getDocCount());
        assertEquals(2, bucket.getDocCountError());

        bucket = terms.getBuckets().get(1);
        assertEquals("A", bucket.getKey().toString());
        assertEquals(4, bucket.getDocCount());
        assertEquals(3, bucket.getDocCountError());
    }
}
