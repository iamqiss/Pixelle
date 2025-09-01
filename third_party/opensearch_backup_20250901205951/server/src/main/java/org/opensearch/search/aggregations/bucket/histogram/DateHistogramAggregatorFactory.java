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

package org.density.search.aggregations.bucket.histogram;

import org.density.common.Rounding;
import org.density.index.query.QueryShardContext;
import org.density.search.aggregations.Aggregator;
import org.density.search.aggregations.AggregatorFactories;
import org.density.search.aggregations.AggregatorFactory;
import org.density.search.aggregations.BucketOrder;
import org.density.search.aggregations.CardinalityUpperBound;
import org.density.search.aggregations.support.CoreValuesSourceType;
import org.density.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.density.search.aggregations.support.ValuesSourceConfig;
import org.density.search.aggregations.support.ValuesSourceRegistry;
import org.density.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Aggregation Factory for date_histogram agg
 *
 * @density.internal
 */
public final class DateHistogramAggregatorFactory extends ValuesSourceAggregatorFactory {

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        builder.register(
            DateHistogramAggregationBuilder.REGISTRY_KEY,
            List.of(CoreValuesSourceType.DATE, CoreValuesSourceType.NUMERIC, CoreValuesSourceType.BOOLEAN),
            DateHistogramAggregator::new,
            true
        );

        builder.register(DateHistogramAggregationBuilder.REGISTRY_KEY, CoreValuesSourceType.RANGE, DateRangeHistogramAggregator::new, true);
    }

    private final BucketOrder order;
    private final boolean keyed;
    private final long minDocCount;
    private final LongBounds extendedBounds;
    private final LongBounds hardBounds;
    private final Rounding rounding;

    public DateHistogramAggregatorFactory(
        String name,
        ValuesSourceConfig config,
        BucketOrder order,
        boolean keyed,
        long minDocCount,
        Rounding rounding,
        LongBounds extendedBounds,
        LongBounds hardBounds,
        QueryShardContext queryShardContext,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, config, queryShardContext, parent, subFactoriesBuilder, metadata);
        this.order = order;
        this.keyed = keyed;
        this.minDocCount = minDocCount;
        this.extendedBounds = extendedBounds;
        this.hardBounds = hardBounds;
        this.rounding = rounding;
    }

    public long minDocCount() {
        return minDocCount;
    }

    protected Aggregator doCreateInternal(
        SearchContext searchContext,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        DateHistogramAggregationSupplier aggregatorSupplier = queryShardContext.getValuesSourceRegistry()
            .getAggregator(DateHistogramAggregationBuilder.REGISTRY_KEY, config);
        // TODO: Is there a reason not to get the prepared rounding in the supplier itself?
        Rounding.Prepared preparedRounding = config.getValuesSource().roundingPreparer(queryShardContext.getIndexReader()).apply(rounding);
        return aggregatorSupplier.build(
            name,
            factories,
            rounding,
            preparedRounding,
            order,
            keyed,
            minDocCount,
            extendedBounds,
            hardBounds,
            config,
            searchContext,
            parent,
            cardinality,
            metadata
        );
    }

    @Override
    protected Aggregator createUnmapped(SearchContext searchContext, Aggregator parent, Map<String, Object> metadata) throws IOException {
        return new DateHistogramAggregator(
            name,
            factories,
            rounding,
            null,
            order,
            keyed,
            minDocCount,
            extendedBounds,
            hardBounds,
            config,
            searchContext,
            parent,
            CardinalityUpperBound.NONE,
            metadata
        );
    }

    @Override
    protected boolean supportsConcurrentSegmentSearch() {
        return true;
    }

    public Rounding.DateTimeUnit getRounding() {
        return this.rounding.unit();
    }
}
