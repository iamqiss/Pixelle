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

package org.density.search.geo;

import org.density.action.search.SearchAction;
import org.density.action.search.SearchPhaseExecutionException;
import org.density.action.search.SearchRequestBuilder;
import org.density.common.geo.GeoShapeType;
import org.density.common.geo.ShapeRelation;
import org.density.common.geo.builders.CoordinatesBuilder;
import org.density.common.geo.builders.LineStringBuilder;
import org.density.common.geo.builders.MultiLineStringBuilder;
import org.density.common.geo.builders.MultiPointBuilder;
import org.density.common.geo.builders.PointBuilder;
import org.density.common.xcontent.XContentFactory;
import org.density.core.xcontent.XContentBuilder;
import org.density.geometry.Line;
import org.density.geometry.LinearRing;
import org.density.geometry.MultiLine;
import org.density.geometry.MultiPoint;
import org.density.geometry.Point;
import org.density.geometry.Rectangle;
import org.density.index.query.GeoShapeQueryBuilder;
import org.density.index.query.QueryBuilders;

import static org.hamcrest.Matchers.containsString;

public class GeoPointShapeQueryTests extends GeoQueryTests {

    @Override
    protected XContentBuilder createTypedMapping() throws Exception {
        XContentBuilder xcb = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type1")
            .startObject("properties")
            .startObject("location")
            .field("type", "geo_point")
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        return xcb;
    }

    @Override
    protected XContentBuilder createDefaultMapping() throws Exception {
        XContentBuilder xcb = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject(defaultGeoFieldName)
            .field("type", "geo_point")
            .endObject()
            .endObject()
            .endObject();

        return xcb;
    }

    public void testProcessRelationSupport() throws Exception {
        XContentBuilder xcb = createDefaultMapping();
        client().admin().indices().prepareCreate("test").setMapping(xcb).get();
        ensureGreen();

        Rectangle rectangle = new Rectangle(-35, -25, -25, -35);

        for (ShapeRelation shapeRelation : ShapeRelation.values()) {
            if (shapeRelation.equals(ShapeRelation.INTERSECTS) == false) {
                try {
                    client().prepareSearch("test")
                        .setQuery(QueryBuilders.geoShapeQuery(defaultGeoFieldName, rectangle).relation(shapeRelation))
                        .get();
                    fail("Expected " + shapeRelation + " query relation not supported for Field [" + defaultGeoFieldName + "]");
                } catch (SearchPhaseExecutionException e) {
                    assertThat(
                        e.getCause().getMessage(),
                        containsString(shapeRelation + " query relation not supported for Field [" + defaultGeoFieldName + "]")
                    );
                }
            }
        }
    }

    public void testQueryLine() throws Exception {
        XContentBuilder xcb = createDefaultMapping();
        client().admin().indices().prepareCreate("test").setMapping(xcb).get();
        ensureGreen();

        Line line = new Line(new double[] { -25, -25 }, new double[] { -35, -35 });

        try {
            client().prepareSearch("test").setQuery(QueryBuilders.geoShapeQuery(defaultGeoFieldName, line)).get();
            fail("Expected field [" + defaultGeoFieldName + "] does not support LINEARRING queries");
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.getCause().getMessage(), containsString("does not support " + GeoShapeType.LINESTRING + " queries"));
        }
    }

    public void testQueryLinearRing() throws Exception {
        XContentBuilder xcb = createDefaultMapping();
        client().admin().indices().prepareCreate("test").setMapping(xcb).get();
        ensureGreen();

        LinearRing linearRing = new LinearRing(new double[] { -25, -35, -25 }, new double[] { -25, -35, -25 });

        try {
            // LinearRing extends Line implements Geometry: expose the build process
            GeoShapeQueryBuilder queryBuilder = new GeoShapeQueryBuilder(defaultGeoFieldName, linearRing);
            SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client(), SearchAction.INSTANCE);
            searchRequestBuilder.setQuery(queryBuilder);
            searchRequestBuilder.setIndices("test");
            searchRequestBuilder.get();
            fail("Expected field [" + defaultGeoFieldName + "] does not support LINEARRING queries");
        } catch (SearchPhaseExecutionException e) {
            assertThat(
                e.getCause().getMessage(),
                containsString("Field [" + defaultGeoFieldName + "] does not support LINEARRING queries")
            );
        }
    }

    public void testQueryMultiLine() throws Exception {
        XContentBuilder xcb = createDefaultMapping();
        client().admin().indices().prepareCreate("test").setMapping(xcb).get();
        ensureGreen();

        CoordinatesBuilder coords1 = new CoordinatesBuilder().coordinate(-35, -35).coordinate(-25, -25);
        CoordinatesBuilder coords2 = new CoordinatesBuilder().coordinate(-15, -15).coordinate(-5, -5);
        LineStringBuilder lsb1 = new LineStringBuilder(coords1);
        LineStringBuilder lsb2 = new LineStringBuilder(coords2);
        MultiLineStringBuilder mlb = new MultiLineStringBuilder().linestring(lsb1).linestring(lsb2);
        MultiLine multiline = (MultiLine) mlb.buildGeometry();

        try {
            client().prepareSearch("test").setQuery(QueryBuilders.geoShapeQuery(defaultGeoFieldName, multiline)).get();
            fail("Expected field [" + defaultGeoFieldName + "] does not support " + GeoShapeType.MULTILINESTRING + " queries");
        } catch (Exception e) {
            assertThat(e.getCause().getMessage(), containsString("does not support " + GeoShapeType.MULTILINESTRING + " queries"));
        }
    }

    public void testQueryMultiPoint() throws Exception {
        XContentBuilder xcb = createDefaultMapping();
        client().admin().indices().prepareCreate("test").setMapping(xcb).get();
        ensureGreen();

        MultiPointBuilder mpb = new MultiPointBuilder().coordinate(-35, -25).coordinate(-15, -5);
        MultiPoint multiPoint = mpb.buildGeometry();

        try {
            client().prepareSearch("test").setQuery(QueryBuilders.geoShapeQuery(defaultGeoFieldName, multiPoint)).get();
            fail("Expected field [" + defaultGeoFieldName + "] does not support " + GeoShapeType.MULTIPOINT + " queries");
        } catch (Exception e) {
            assertThat(e.getCause().getMessage(), containsString("does not support " + GeoShapeType.MULTIPOINT + " queries"));
        }
    }

    public void testQueryPoint() throws Exception {
        XContentBuilder xcb = createDefaultMapping();
        client().admin().indices().prepareCreate("test").setMapping(xcb).get();
        ensureGreen();

        PointBuilder pb = new PointBuilder().coordinate(-35, -25);
        Point point = pb.buildGeometry();

        try {
            client().prepareSearch("test").setQuery(QueryBuilders.geoShapeQuery(defaultGeoFieldName, point)).get();
            fail("Expected field [" + defaultGeoFieldName + "] does not support " + GeoShapeType.POINT + " queries");
        } catch (Exception e) {
            assertThat(e.getCause().getMessage(), containsString("does not support " + GeoShapeType.POINT + " queries"));
        }
    }
}
