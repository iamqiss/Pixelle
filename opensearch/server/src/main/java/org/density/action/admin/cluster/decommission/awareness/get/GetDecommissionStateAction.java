/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.cluster.decommission.awareness.get;

import org.density.action.ActionType;

/**
 * Get decommission action
 *
 * @density.internal
 */
public class GetDecommissionStateAction extends ActionType<GetDecommissionStateResponse> {

    public static final GetDecommissionStateAction INSTANCE = new GetDecommissionStateAction();
    public static final String NAME = "cluster:admin/decommission/awareness/get";

    private GetDecommissionStateAction() {
        super(NAME, GetDecommissionStateResponse::new);
    }
}
