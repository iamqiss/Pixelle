/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.index.codec.composite;

import org.density.common.annotation.ExperimentalApi;
import org.density.index.compositeindex.datacube.startree.index.CompositeIndexValues;

import java.io.IOException;
import java.util.List;

/**
 * Interface that abstracts the functionality to read composite index structures from the segment
 *
 * @density.experimental
 */
@ExperimentalApi
public interface CompositeIndexReader {
    /**
     * Get list of composite index fields from the segment
     *
     */
    List<CompositeIndexFieldInfo> getCompositeIndexFields();

    /**
     * Get composite index values based on the field name and the field type
     */
    CompositeIndexValues getCompositeIndexValues(CompositeIndexFieldInfo fieldInfo) throws IOException;
}
