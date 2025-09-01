/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.common.cache;

import org.density.common.annotation.PublicApi;

/**
 * Reason for notification removal
 *
 * @density.api
 */
@PublicApi(since = "1.0.0")
public enum RemovalReason {
    REPLACED,
    INVALIDATED,
    EVICTED,
    EXPLICIT,
    CAPACITY,
    RESTARTED // This is used by testing framework to close the CachedIndexInput during node restart.
}
