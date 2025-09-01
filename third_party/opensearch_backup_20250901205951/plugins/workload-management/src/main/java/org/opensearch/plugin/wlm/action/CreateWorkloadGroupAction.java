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
 * Transport action to create WorkloadGroup
 *
 * @density.experimental
 */
public class CreateWorkloadGroupAction extends ActionType<CreateWorkloadGroupResponse> {

    /**
     * An instance of CreateWorkloadGroupAction
     */
    public static final CreateWorkloadGroupAction INSTANCE = new CreateWorkloadGroupAction();

    /**
     * Name for CreateWorkloadGroupAction
     */
    public static final String NAME = "cluster:admin/density/wlm/workload_group/_create";

    /**
     * Default constructor
     */
    private CreateWorkloadGroupAction() {
        super(NAME, CreateWorkloadGroupResponse::new);
    }
}
