/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.arrow.flight.stats;

import org.density.action.ActionType;

/**
 * Action for retrieving Flight transport statistics
 */
public class FlightStatsAction extends ActionType<FlightStatsResponse> {

    /** Singleton instance */
    public static final FlightStatsAction INSTANCE = new FlightStatsAction();
    /** Action name */
    public static final String NAME = "cluster:monitor/flight/stats";

    private FlightStatsAction() {
        super(NAME, FlightStatsResponse::new);
    }
}
