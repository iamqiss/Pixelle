/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.cluster.wlm;

import org.density.action.ActionType;

/**
 * Transport action for obtaining Workload Management Stats.
 *
 * @density.experimental
 */
public class WlmStatsAction extends ActionType<WlmStatsResponse> {
    public static final WlmStatsAction INSTANCE = new WlmStatsAction();
    public static final String NAME = "cluster:monitor/wlm/stats";

    private WlmStatsAction() {
        super(NAME, WlmStatsResponse::new);
    }
}
