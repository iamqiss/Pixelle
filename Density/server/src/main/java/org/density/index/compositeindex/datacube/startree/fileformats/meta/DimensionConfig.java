/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.index.compositeindex.datacube.startree.fileformats.meta;

import org.apache.lucene.index.DocValuesType;
import org.density.common.annotation.ExperimentalApi;
import org.density.index.compositeindex.datacube.DimensionDataType;

/**
 * Class to store DocValuesType and DimensionDataType for a dimension.
 *
 * @density.experimental
 */
@ExperimentalApi
public class DimensionConfig {

    private final DocValuesType docValuesType;
    private final DimensionDataType dimensionDataType;

    public DimensionConfig(DocValuesType docValuesType, DimensionDataType dimensionDataType) {
        this.docValuesType = docValuesType;
        this.dimensionDataType = dimensionDataType;
    }

    public DocValuesType getDocValuesType() {
        return docValuesType;
    }

    public DimensionDataType getDimensionDataType() {
        return dimensionDataType;
    }
}
