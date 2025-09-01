/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.density.index.compositeindex.datacube.startree.aggregators;

import org.density.index.mapper.FieldValueConverter;

/**
 * Max value aggregator for star tree
 *
 * @density.experimental
 */
class MaxValueAggregator extends StatelessDoubleValueAggregator {

    public MaxValueAggregator(FieldValueConverter fieldValueConverter) {
        super(fieldValueConverter, null);
    }

    @Override
    protected Double performValueAggregation(Double aggregatedValue, Double segmentDocValue) {
        return Math.max(aggregatedValue, segmentDocValue);
    }
}
