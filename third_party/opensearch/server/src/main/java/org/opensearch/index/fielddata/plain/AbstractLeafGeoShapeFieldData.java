/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.index.fielddata.plain;

import org.apache.lucene.document.LatLonShapeDocValuesField;
import org.density.index.fielddata.FieldData;
import org.density.index.fielddata.LeafGeoShapeFieldData;
import org.density.index.fielddata.ScriptDocValues;
import org.density.index.fielddata.SortedBinaryDocValues;
import org.density.search.aggregations.AggregationExecutionException;

/**
 * Base class for retrieving GeoShape doc values which are added as {@link LatLonShapeDocValuesField} in Lucene
 */
public abstract class AbstractLeafGeoShapeFieldData implements LeafGeoShapeFieldData {

    /**
     * Return a String representation of the values.
     */
    @Override
    public final SortedBinaryDocValues getBytesValues() {
        return FieldData.toString(getGeoShapeValue());
    }

    /**
     * Returns field values for use in scripting. We don't support Script values in the GeoShape for now.
     * Code should not come to this place, as we have added not to support this at:
     * CoreValuesSourceTypeGEO_SHAPE
     */
    @Override
    public final ScriptDocValues<?> getScriptValues() {
        // TODO: https://github.com/density-project/geospatial/issues/128
        throw new AggregationExecutionException("Script doc value for the GeoShape field is not supported");
    }
}
