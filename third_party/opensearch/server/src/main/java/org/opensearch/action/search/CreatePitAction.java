/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.search;

import org.density.action.ActionType;

/**
 * Action type for creating PIT reader context
 */
public class CreatePitAction extends ActionType<CreatePitResponse> {
    public static final CreatePitAction INSTANCE = new CreatePitAction();
    public static final String NAME = "indices:data/read/point_in_time/create";

    private CreatePitAction() {
        super(NAME, CreatePitResponse::new);
    }
}
