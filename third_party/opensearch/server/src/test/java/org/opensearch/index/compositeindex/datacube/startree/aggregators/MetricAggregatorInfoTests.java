/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.index.compositeindex.datacube.startree.aggregators;

import org.density.index.compositeindex.datacube.MetricStat;
import org.density.index.mapper.NumberFieldMapper;
import org.density.test.DensityTestCase;

public class MetricAggregatorInfoTests extends DensityTestCase {

    public void testConstructor() {
        MetricAggregatorInfo pair = new MetricAggregatorInfo(
            MetricStat.SUM,
            "column1",
            "star_tree_field",
            NumberFieldMapper.NumberType.DOUBLE
        );
        assertEquals(MetricStat.SUM, pair.getMetricStat());
        assertEquals("column1", pair.getField());
    }

    public void testCountStarConstructor() {
        MetricAggregatorInfo pair = new MetricAggregatorInfo(
            MetricStat.VALUE_COUNT,
            "anything",
            "star_tree_field",
            NumberFieldMapper.NumberType.DOUBLE
        );
        assertEquals(MetricStat.VALUE_COUNT, pair.getMetricStat());
        assertEquals("anything", pair.getField());
    }

    public void testToFieldName() {
        MetricAggregatorInfo pair = new MetricAggregatorInfo(
            MetricStat.SUM,
            "column2",
            "star_tree_field",
            NumberFieldMapper.NumberType.DOUBLE
        );
        assertEquals("star_tree_field_column2_sum", pair.toFieldName());
    }

    public void testEquals() {
        MetricAggregatorInfo pair1 = new MetricAggregatorInfo(
            MetricStat.SUM,
            "column1",
            "star_tree_field",
            NumberFieldMapper.NumberType.DOUBLE
        );
        MetricAggregatorInfo pair2 = new MetricAggregatorInfo(
            MetricStat.SUM,
            "column1",
            "star_tree_field",
            NumberFieldMapper.NumberType.DOUBLE
        );
        assertEquals(pair1, pair2);
        assertNotEquals(
            pair1,
            new MetricAggregatorInfo(MetricStat.VALUE_COUNT, "column1", "star_tree_field", NumberFieldMapper.NumberType.DOUBLE)
        );
        assertNotEquals(pair1, new MetricAggregatorInfo(MetricStat.SUM, "column2", "star_tree_field", NumberFieldMapper.NumberType.DOUBLE));
    }

    public void testHashCode() {
        MetricAggregatorInfo pair1 = new MetricAggregatorInfo(
            MetricStat.SUM,
            "column1",
            "star_tree_field",
            NumberFieldMapper.NumberType.DOUBLE
        );
        MetricAggregatorInfo pair2 = new MetricAggregatorInfo(
            MetricStat.SUM,
            "column1",
            "star_tree_field",
            NumberFieldMapper.NumberType.DOUBLE
        );
        assertEquals(pair1.hashCode(), pair2.hashCode());
    }

    public void testCompareTo() {
        MetricAggregatorInfo pair1 = new MetricAggregatorInfo(
            MetricStat.SUM,
            "column1",
            "star_tree_field",
            NumberFieldMapper.NumberType.DOUBLE
        );
        MetricAggregatorInfo pair2 = new MetricAggregatorInfo(
            MetricStat.SUM,
            "column2",
            "star_tree_field",
            NumberFieldMapper.NumberType.DOUBLE
        );
        MetricAggregatorInfo pair3 = new MetricAggregatorInfo(
            MetricStat.VALUE_COUNT,
            "column1",
            "star_tree_field",
            NumberFieldMapper.NumberType.DOUBLE
        );
        assertTrue(pair1.compareTo(pair2) < 0);
        assertTrue(pair2.compareTo(pair1) > 0);
        assertTrue(pair1.compareTo(pair3) > 0);
        assertTrue(pair3.compareTo(pair1) < 0);
    }
}
