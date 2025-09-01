/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.cluster.decommission.awareness.put;

import org.density.action.ActionType;

/**
 * Register decommission action
 *
 * @density.internal
 */
public final class DecommissionAction extends ActionType<DecommissionResponse> {
    public static final DecommissionAction INSTANCE = new DecommissionAction();
    public static final String NAME = "cluster:admin/decommission/awareness/put";

    private DecommissionAction() {
        super(NAME, DecommissionResponse::new);
    }
}
