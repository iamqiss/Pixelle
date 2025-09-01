/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.search.profile;

import org.density.common.annotation.ExperimentalApi;

import java.util.Map;

/**
 * A metric for profiling.
 */
@ExperimentalApi
public abstract class ProfileMetric {

    private final String name;

    public ProfileMetric(String name) {
        this.name = name;
    }

    /**
     *
     * @return name of the metric
     */
    public String getName() {
        return name;
    }

    /**
     *
     * @return map representation of breakdown
     */
    abstract public Map<String, Long> toBreakdownMap();
}
