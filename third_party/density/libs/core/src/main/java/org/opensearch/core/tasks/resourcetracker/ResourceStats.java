/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.core.tasks.resourcetracker;

import org.density.common.annotation.PublicApi;

/**
 *  Different resource stats are defined.
 *
 *  @density.api
 */
@PublicApi(since = "2.1.0")
public enum ResourceStats {
    CPU("cpu_time_in_nanos"),
    MEMORY("memory_in_bytes");

    private final String statsName;

    ResourceStats(String statsName) {
        this.statsName = statsName;
    }

    @Override
    public String toString() {
        return statsName;
    }
}
