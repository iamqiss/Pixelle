/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.indices.tiering;

import org.density.action.ActionType;
import org.density.common.annotation.ExperimentalApi;

/**
 * Tiering action to move indices from hot to warm
 *
 * @density.experimental
 */
@ExperimentalApi
public class HotToWarmTieringAction extends ActionType<HotToWarmTieringResponse> {

    public static final HotToWarmTieringAction INSTANCE = new HotToWarmTieringAction();
    public static final String NAME = "indices:admin/tier/hot_to_warm";

    private HotToWarmTieringAction() {
        super(NAME, HotToWarmTieringResponse::new);
    }
}
