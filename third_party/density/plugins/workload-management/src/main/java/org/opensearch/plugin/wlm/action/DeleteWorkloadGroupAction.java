/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.plugin.wlm.action;

import org.density.action.ActionType;
import org.density.action.support.clustermanager.AcknowledgedResponse;

/**
 * Transport action for delete WorkloadGroup
 *
 * @density.experimental
 */
public class DeleteWorkloadGroupAction extends ActionType<AcknowledgedResponse> {

    /**
     /**
     * An instance of DeleteWorkloadGroupAction
     */
    public static final DeleteWorkloadGroupAction INSTANCE = new DeleteWorkloadGroupAction();

    /**
     * Name for DeleteWorkloadGroupAction
     */
    public static final String NAME = "cluster:admin/density/wlm/workload_group/_delete";

    /**
     * Default constructor
     */
    private DeleteWorkloadGroupAction() {
        super(NAME, AcknowledgedResponse::new);
    }
}
