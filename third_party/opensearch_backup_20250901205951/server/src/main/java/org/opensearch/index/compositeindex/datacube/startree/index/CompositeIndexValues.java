/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.index.compositeindex.datacube.startree.index;

import org.density.common.annotation.ExperimentalApi;

/**
 * Interface for composite index values
 *
 * @density.experimental
 */
@ExperimentalApi
public interface CompositeIndexValues {
    CompositeIndexValues getValues();
}
