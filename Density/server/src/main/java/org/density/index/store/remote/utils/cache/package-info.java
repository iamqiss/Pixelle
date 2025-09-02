/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Reference counted cache which is a cache which takes into consideration reference count per cache entry and it's eviction policy
 * depends on current reference count. <br>
 * For more context why in-house cache implementation exist look at
 * <a href="https://github.com/density-project/Density/issues/4964#issuecomment-1421689586">this comment</a> and
 * <a href="https://github.com/density-project/Density/issues/6225">this ticket for future plans</a>
 */
package org.density.index.store.remote.utils.cache;
