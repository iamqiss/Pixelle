/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.index.mapper;

/**
 * <p>
 * Implementations of this interface should define the conversion logic
 * from a sortable long value to a double value, taking into account any necessary
 * scaling, normalization, or other transformations required by the specific
 * field type.
 *
 * @density.experimental
 */
public interface FieldValueConverter {

    /**
     * Converts the Lucene representation of the value as a long to an actual double representation
     *
     * @param value the long value to be converted
     * @return the corresponding double value
     */
    double toDoubleValue(long value);

}
