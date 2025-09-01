/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.index.query;

/**
 * Interface for classes with a fieldName method
 *
 * @density.internal
 */
public interface WithFieldName {
    /**
     * Get the field name for this query.
     */
    String fieldName();
}
