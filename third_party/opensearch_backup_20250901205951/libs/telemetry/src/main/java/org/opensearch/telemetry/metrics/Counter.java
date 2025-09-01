/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.telemetry.metrics;

import org.density.common.annotation.ExperimentalApi;
import org.density.telemetry.metrics.tags.Tags;

/**
 * Counter adds the value to the existing metric.
 * {@density.experimental}
 */
@ExperimentalApi
public interface Counter {

    /**
     * add value.
     * @param value value to be added.
     */
    void add(double value);

    /**
     * add value along with the attributes.
     *
     * @param value value to be added.
     * @param tags  attributes/dimensions of the metric.
     */
    void add(double value, Tags tags);

}
