/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.common.cache;

import org.density.common.annotation.ExperimentalApi;

/**
 * Extends a cache loader with awareness of whether the data is loaded or not.
 * @param <K> Type of key.
 * @param <V> Type of value.
 *
 * @density.experimental
 */
@ExperimentalApi
public interface LoadAwareCacheLoader<K, V> extends CacheLoader<K, V> {
    boolean isLoaded();
}
