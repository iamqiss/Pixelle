/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.search.aggregations.metrics;

import org.density.index.compositeindex.datacube.MetricStat;
import org.density.index.query.QueryShardContext;
import org.density.search.aggregations.AggregatorFactories;
import org.density.search.aggregations.AggregatorFactory;
import org.density.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.density.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.Map;

/**
 * Extending ValuesSourceAggregatorFactory for aggregation factories supported by star-tree implementation
 */
public abstract class MetricAggregatorFactory extends ValuesSourceAggregatorFactory {
    public MetricAggregatorFactory(
        String name,
        ValuesSourceConfig config,
        QueryShardContext queryShardContext,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, config, queryShardContext, parent, subFactoriesBuilder, metadata);
    }

    public abstract MetricStat getMetricStat();
}
