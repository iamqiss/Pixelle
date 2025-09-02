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
import org.density.search.aggregations.CardinalityUpperBound;
import org.density.search.aggregations.bucket.histogram.AutoDateHistogramAggregationBuilder.RoundingInfo;
import org.density.search.aggregations.support.CoreValuesSourceType;
import org.density.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.density.search.aggregations.support.ValuesSourceConfig;
import org.density.search.aggregations.support.ValuesSourceRegistry;
import org.density.search.internal.SearchContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;

/**
 * Aggregation Factory for auto_date_histogram
 *
 * @density.internal
 */
public final class AutoDateHistogramAggregatorFactory extends ValuesSourceAggregatorFactory {

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        builder.register(
            AutoDateHistogramAggregationBuilder.REGISTRY_KEY,
            Arrays.asList(CoreValuesSourceType.DATE, CoreValuesSourceType.NUMERIC, CoreValuesSourceType.BOOLEAN),
            AutoDateHistogramAggregator::build,
            true
        );
    }

    private final int numBuckets;
    private RoundingInfo[] roundingInfos;

    public AutoDateHistogramAggregatorFactory(
        String name,
        ValuesSourceConfig config,
        int numBuckets,
        RoundingInfo[] roundingInfos,
        QueryShardContext queryShardContext,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, config, queryShardContext, parent, subFactoriesBuilder, metadata);
        this.numBuckets = numBuckets;
        this.roundingInfos = roundingInfos;
    }

    @Override
    protected Aggregator doCreateInternal(
        SearchContext searchContext,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        AutoDateHistogramAggregatorSupplier aggregatorSupplier = queryShardContext.getValuesSourceRegistry()
            .getAggregator(AutoDateHistogramAggregationBuilder.REGISTRY_KEY, config);
        Function<Rounding, Rounding.Prepared> roundingPreparer = config.getValuesSource()
            .roundingPreparer(searchContext.getQueryShardContext().getIndexReader());
        return aggregatorSupplier.build(
            name,
            factories,
            numBuckets,
            roundingInfos,
            roundingPreparer,
            config,
            searchContext,
            parent,
            cardinality,
            metadata
        );
    }

    @Override
    protected Aggregator createUnmapped(SearchContext searchContext, Aggregator parent, Map<String, Object> metadata) throws IOException {
        return AutoDateHistogramAggregator.build(
            name,
            factories,
            numBuckets,
            roundingInfos,
            Rounding::prepareForUnknown,
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
}
