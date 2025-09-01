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

package org.density.index.query;

import org.apache.lucene.document.LatLonShape;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.density.LegacyESVersion;
import org.density.common.geo.GeoLineDecomposer;
import org.density.common.geo.GeoPolygonDecomposer;
import org.density.common.geo.GeoShapeUtils;
import org.density.common.geo.ShapeRelation;
import org.density.geometry.Circle;
import org.density.geometry.Geometry;
import org.density.geometry.GeometryCollection;
import org.density.geometry.GeometryVisitor;
import org.density.geometry.Line;
import org.density.geometry.LinearRing;
import org.density.geometry.MultiLine;
import org.density.geometry.MultiPoint;
import org.density.geometry.MultiPolygon;
import org.density.geometry.Point;
import org.density.geometry.Polygon;
import org.density.geometry.Rectangle;

import java.util.ArrayList;
import java.util.List;

/**
 * Query processor for Lucene 6 LatLonShape queries
 *
 * @density.internal
 */
public class VectorGeoShapeQueryProcessor {

    public Query geoShapeQuery(Geometry shape, String fieldName, ShapeRelation relation, QueryShardContext context) {
        // CONTAINS queries are not supported by VECTOR strategy for indices created before version 7.5.0 (Lucene 8.3.0)
        if (relation == ShapeRelation.CONTAINS && context.indexVersionCreated().before(LegacyESVersion.fromId(7050099))) {
            throw new QueryShardException(context, ShapeRelation.CONTAINS + " query relation not supported for Field [" + fieldName + "].");
        }
        // wrap geoQuery as a ConstantScoreQuery
        return getVectorQueryFromShape(shape, fieldName, relation, context);
    }

    private Query getVectorQueryFromShape(Geometry queryShape, String fieldName, ShapeRelation relation, QueryShardContext context) {
        final LuceneGeometryCollector visitor = new LuceneGeometryCollector(fieldName, context);
        queryShape.visit(visitor);
        final List<LatLonGeometry> geometries = visitor.geometries();
        if (geometries.size() == 0) {
            return new MatchNoDocsQuery();
        }
        return LatLonShape.newGeometryQuery(fieldName, relation.getLuceneRelation(), geometries.toArray(new LatLonGeometry[0]));
    }

    /**
     * Geometry collector for LatLonShape indexing types
     *
     * @density.internal
     */
    private static class LuceneGeometryCollector implements GeometryVisitor<Void, RuntimeException> {
        private final List<LatLonGeometry> geometries = new ArrayList<>();
        private final String name;
        private final QueryShardContext context;

        private LuceneGeometryCollector(String name, QueryShardContext context) {
            this.name = name;
            this.context = context;
        }

        List<LatLonGeometry> geometries() {
            return geometries;
        }

        @Override
        public Void visit(Circle circle) {
            if (circle.isEmpty() == false) {
                geometries.add(GeoShapeUtils.toLuceneCircle(circle));
            }
            return null;
        }

        @Override
        public Void visit(GeometryCollection<?> collection) {
            for (Geometry shape : collection) {
                shape.visit(this);
            }
            return null;
        }

        @Override
        public Void visit(org.density.geometry.Line line) {
            if (line.isEmpty() == false) {
                List<org.density.geometry.Line> collector = new ArrayList<>();
                GeoLineDecomposer.decomposeLine(line, collector);
                collectLines(collector);
            }
            return null;
        }

        @Override
        public Void visit(LinearRing ring) {
            throw new QueryShardException(context, "Field [" + name + "] found and unsupported shape LinearRing");
        }

        @Override
        public Void visit(MultiLine multiLine) {
            List<org.density.geometry.Line> collector = new ArrayList<>();
            GeoLineDecomposer.decomposeMultiLine(multiLine, collector);
            collectLines(collector);
            return null;
        }

        @Override
        public Void visit(MultiPoint multiPoint) {
            for (Point point : multiPoint) {
                visit(point);
            }
            return null;
        }

        @Override
        public Void visit(MultiPolygon multiPolygon) {
            if (multiPolygon.isEmpty() == false) {
                List<org.density.geometry.Polygon> collector = new ArrayList<>();
                GeoPolygonDecomposer.decomposeMultiPolygon(multiPolygon, true, collector);
                collectPolygons(collector);
            }
            return null;
        }

        @Override
        public Void visit(Point point) {
            if (point.isEmpty() == false) {
                // points are a special "shape" case: for queries we need to quantize since the Lucene
                // tessellator doesn't do anything with them.
                // todo this is a sandy lucene experience so we should investigate fixing this upstream
                double quantizedLat = GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(point.getLat()));
                double quantizedLon = GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(point.getLon()));
                geometries.add(new org.apache.lucene.geo.Point(quantizedLat, quantizedLon));
            }
            return null;

        }

        @Override
        public Void visit(org.density.geometry.Polygon polygon) {
            if (polygon.isEmpty() == false) {
                List<org.density.geometry.Polygon> collector = new ArrayList<>();
                GeoPolygonDecomposer.decomposePolygon(polygon, true, collector);
                collectPolygons(collector);
            }
            return null;
        }

        @Override
        public Void visit(Rectangle r) {
            if (r.isEmpty() == false) {
                geometries.add(GeoShapeUtils.toLuceneRectangle(r));
            }
            return null;
        }

        private void collectLines(List<org.density.geometry.Line> geometryLines) {
            for (Line line : geometryLines) {
                geometries.add(GeoShapeUtils.toLuceneLine(line));
            }
        }

        private void collectPolygons(List<org.density.geometry.Polygon> geometryPolygons) {
            for (Polygon polygon : geometryPolygons) {
                geometries.add(GeoShapeUtils.toLucenePolygon(polygon));
            }
        }
    }
}
