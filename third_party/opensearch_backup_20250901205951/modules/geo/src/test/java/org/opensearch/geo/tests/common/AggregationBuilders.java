/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.geo.tests.common;

import org.density.geo.search.aggregations.bucket.geogrid.GeoHashGrid;
import org.density.geo.search.aggregations.bucket.geogrid.GeoHashGridAggregationBuilder;
import org.density.geo.search.aggregations.bucket.geogrid.GeoTileGrid;
import org.density.geo.search.aggregations.bucket.geogrid.GeoTileGridAggregationBuilder;
import org.density.geo.search.aggregations.metrics.GeoBounds;
import org.density.geo.search.aggregations.metrics.GeoBoundsAggregationBuilder;

public class AggregationBuilders {
    /**
     * Create a new {@link GeoBounds} aggregation with the given name.
     */
    public static GeoBoundsAggregationBuilder geoBounds(String name) {
        return new GeoBoundsAggregationBuilder(name);
    }

    /**
     * Create a new {@link GeoHashGrid} aggregation with the given name.
     */
    public static GeoHashGridAggregationBuilder geohashGrid(String name) {
        return new GeoHashGridAggregationBuilder(name);
    }

    /**
     * Create a new {@link GeoTileGrid} aggregation with the given name.
     */
    public static GeoTileGridAggregationBuilder geotileGrid(String name) {
        return new GeoTileGridAggregationBuilder(name);
    }
}
