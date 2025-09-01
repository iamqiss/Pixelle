/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.index.fielddata;

/**
 * {@link LeafFieldData} specialization for geo shapes.
 *
 * @density.internal
 */
public interface LeafGeoShapeFieldData extends LeafFieldData {

    /**
     * Return the appropriate instance that can be used to read the Geo shape values from lucene.
     *
     * @return {@link GeoShapeValue}
     */
    GeoShapeValue getGeoShapeValue();
}
