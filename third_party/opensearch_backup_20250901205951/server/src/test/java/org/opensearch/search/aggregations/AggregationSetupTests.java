/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.search.aggregations;

import org.density.action.search.SearchShardTask;
import org.density.common.xcontent.json.JsonXContent;
import org.density.core.xcontent.XContentParser;
import org.density.index.IndexService;
import org.density.search.internal.SearchContext;
import org.density.test.DensitySingleNodeTestCase;
import org.density.test.TestSearchContext;

import java.io.IOException;

public class AggregationSetupTests extends DensitySingleNodeTestCase {
    protected IndexService index;

    protected SearchContext context;

    protected final String globalNonGlobalAggs = "{ \"my_terms\": {\"terms\": {\"field\": \"f\"}}, "
        + "\"all_products\": {\"global\": {}, \"aggs\": {\"avg_price\": {\"avg\": { \"field\": \"f\"}}}}}";

    protected final String multipleNonGlobalAggs = "{ \"my_terms\": {\"terms\": {\"field\": \"f\"}}, "
        + "\"avg_price\": {\"avg\": { \"field\": \"f\"}}}";

    protected final String globalAgg = "{ \"all_products\": {\"global\": {}, \"aggs\": {\"avg_price\": {\"avg\": { \"field\": \"f\"}}}}}";

    @Override
    public void setUp() throws Exception {
        super.setUp();
        index = createIndex("idx");
        client().prepareIndex("idx").setId("1").setSource("f", 5).execute().get();
        client().admin().indices().prepareRefresh("idx").get();
        context = createSearchContext(index);
        ((TestSearchContext) context).setConcurrentSegmentSearchEnabled(true);
        SearchShardTask task = new SearchShardTask(0, "n/a", "n/a", "test-kind", null, null);
        context.setTask(task);
    }

    protected AggregatorFactories getAggregationFactories(String agg) throws IOException {
        try (XContentParser aggParser = createParser(JsonXContent.jsonXContent, agg)) {
            aggParser.nextToken();
            return AggregatorFactories.parseAggregators(aggParser).build(context.getQueryShardContext(), null);
        }
    }
}
