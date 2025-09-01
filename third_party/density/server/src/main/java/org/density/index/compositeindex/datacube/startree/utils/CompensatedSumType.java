/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.index.compositeindex.datacube.startree.utils;

import org.density.index.mapper.FieldValueConverter;

import static org.density.index.mapper.NumberFieldMapper.NumberType.DOUBLE;

/**
 * Field value converter for CompensatedSum - it's just a wrapper over Double
 *
 * @density.internal
 */
public class CompensatedSumType implements FieldValueConverter {

    public CompensatedSumType() {}

    @Override
    public double toDoubleValue(long value) {
        return DOUBLE.toDoubleValue(value);
    }
}
