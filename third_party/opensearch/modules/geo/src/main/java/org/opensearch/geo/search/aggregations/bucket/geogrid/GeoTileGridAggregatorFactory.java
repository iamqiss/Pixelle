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

package org.density.geo.search.aggregations.bucket.geogrid;

import org.density.common.geo.GeoBoundingBox;
import org.density.geo.search.aggregations.bucket.geogrid.cells.CellIdSource;
import org.density.geo.search.aggregations.bucket.geogrid.cells.GeoShapeCellIdSource;
import org.density.index.query.QueryShardContext;
import org.density.search.aggregations.Aggregator;
import org.density.search.aggregations.AggregatorFactories;
import org.density.search.aggregations.AggregatorFactory;
import org.density.search.aggregations.CardinalityUpperBound;
import org.density.search.aggregations.InternalAggregation;
import org.density.search.aggregations.NonCollectingAggregator;
import org.density.search.aggregations.bucket.GeoTileUtils;
import org.density.search.aggregations.support.CoreValuesSourceType;
import org.density.search.aggregations.support.ValuesSource;
import org.density.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.density.search.aggregations.support.ValuesSourceConfig;
import org.density.search.aggregations.support.ValuesSourceRegistry;
import org.density.search.internal.SearchContext;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * Aggregation Factory for geo_tile grid agg
 *
 * @density.internal
 */
class GeoTileGridAggregatorFactory extends ValuesSourceAggregatorFactory {

    private final int precision;
    private final int requiredSize;
    private final int shardSize;
    private final GeoBoundingBox geoBoundingBox;

    GeoTileGridAggregatorFactory(
        String name,
        ValuesSourceConfig config,
        int precision,
        int requiredSize,
        int shardSize,
        GeoBoundingBox geoBoundingBox,
        QueryShardContext queryShardContext,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, config, queryShardContext, parent, subFactoriesBuilder, metadata);
        this.precision = precision;
        this.requiredSize = requiredSize;
        this.shardSize = shardSize;
        this.geoBoundingBox = geoBoundingBox;
    }

    @Override
    protected Aggregator createUnmapped(SearchContext searchContext, Aggregator parent, Map<String, Object> metadata) throws IOException {
        final InternalAggregation aggregation = new GeoTileGrid(name, requiredSize, Collections.emptyList(), metadata);
        return new NonCollectingAggregator(name, searchContext, parent, factories, metadata) {
            @Override
            public InternalAggregation buildEmptyAggregation() {
                return aggregation;
            }
        };
    }

    @Override
    protected Aggregator doCreateInternal(
        SearchContext searchContext,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        return queryShardContext.getValuesSourceRegistry()
            .getAggregator(GeoTileGridAggregationBuilder.REGISTRY_KEY, config)
            .build(
                name,
                factories,
                config.getValuesSource(),
                precision,
                geoBoundingBox,
                requiredSize,
                shardSize,
                searchContext,
                parent,
                cardinality,
                metadata
            );
    }

    static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        builder.register(
            GeoTileGridAggregationBuilder.REGISTRY_KEY,
            CoreValuesSourceType.GEOPOINT,
            (
                name,
                factories,
                valuesSource,
                precision,
                geoBoundingBox,
                requiredSize,
                shardSize,
                aggregationContext,
                parent,
                cardinality,
                metadata) -> {
                CellIdSource cellIdSource = new CellIdSource(
                    (ValuesSource.GeoPoint) valuesSource,
                    precision,
                    geoBoundingBox,
                    GeoTileUtils::longEncode
                );
                return new GeoTileGridAggregator(
                    name,
                    factories,
                    cellIdSource,
                    requiredSize,
                    shardSize,
                    aggregationContext,
                    parent,
                    cardinality,
                    metadata
                );
            },
            true
        );

        // registers Aggregation on GeoShape
        builder.register(
            GeoTileGridAggregationBuilder.REGISTRY_KEY,
            CoreValuesSourceType.GEO_SHAPE,
            (
                name,
                factories,
                valuesSource,
                precision,
                geoBoundingBox,
                requiredSize,
                shardSize,
                aggregationContext,
                parent,
                cardinality,
                metadata) -> {
                GeoShapeCellIdSource cellIdSource = new GeoShapeCellIdSource(
                    (ValuesSource.GeoShape) valuesSource,
                    precision,
                    geoBoundingBox,
                    GeoTileUtils::encodeShape
                );
                return new GeoTileGridAggregator(
                    name,
                    factories,
                    cellIdSource,
                    requiredSize,
                    shardSize,
                    aggregationContext,
                    parent,
                    cardinality,
                    metadata
                );
            },
            true
        );
    }

    @Override
    protected boolean supportsConcurrentSegmentSearch() {
        return true;
    }
}
