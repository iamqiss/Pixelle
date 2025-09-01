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
 * Defines the different types of resource stats.
 *
 * @density.api
 */
@PublicApi(since = "2.1.0")
public enum ResourceStatsType {
    // resource stats of the worker thread reported directly from runnable.
    WORKER_STATS("worker_stats", false);

    private final String statsType;
    private final boolean onlyForAnalysis;

    ResourceStatsType(String statsType, boolean onlyForAnalysis) {
        this.statsType = statsType;
        this.onlyForAnalysis = onlyForAnalysis;
    }

    public boolean isOnlyForAnalysis() {
        return onlyForAnalysis;
    }

    @Override
    public String toString() {
        return statsType;
    }
}
