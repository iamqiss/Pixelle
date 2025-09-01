/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.common.round;

import org.density.common.annotation.InternalApi;

/**
 * Interface to round-off values.
 *
 * @density.internal
 */
@InternalApi
@FunctionalInterface
public interface Roundable {
    /**
     * Returns the greatest lower bound of the given key.
     * In other words, it returns the largest value such that {@code value <= key}.
     * @param key to floor
     * @return the floored value
     */
    long floor(long key);
}
