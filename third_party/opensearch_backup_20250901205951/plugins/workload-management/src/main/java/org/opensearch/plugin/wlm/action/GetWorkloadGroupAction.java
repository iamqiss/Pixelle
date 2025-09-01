/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.plugin.wlm.action;

import org.density.action.ActionType;

/**
 * Transport action to get WorkloadGroup
 *
 * @density.experimental
 */
public class GetWorkloadGroupAction extends ActionType<GetWorkloadGroupResponse> {

    /**
     * An instance of GetWorkloadGroupAction
     */
    public static final GetWorkloadGroupAction INSTANCE = new GetWorkloadGroupAction();

    /**
     * Name for GetWorkloadGroupAction
     */
    public static final String NAME = "cluster:admin/density/wlm/workload_group/_get";

    /**
     * Default constructor
     */
    private GetWorkloadGroupAction() {
        super(NAME, GetWorkloadGroupResponse::new);
    }
}
