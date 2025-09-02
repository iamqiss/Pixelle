/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.cluster.decommission.awareness.delete;

import org.density.action.ActionType;

/**
 *  Delete decommission state action.
 *
 * @density.internal
 */
public class DeleteDecommissionStateAction extends ActionType<DeleteDecommissionStateResponse> {
    public static final DeleteDecommissionStateAction INSTANCE = new DeleteDecommissionStateAction();
    public static final String NAME = "cluster:admin/decommission/awareness/delete";

    private DeleteDecommissionStateAction() {
        super(NAME, DeleteDecommissionStateResponse::new);
    }
}
