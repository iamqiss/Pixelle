/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.support.clustermanager.term;

import org.density.action.ActionType;

/**
 * Transport action for fetching cluster term and version
 *
 * @density.internal
 */
public class GetTermVersionAction extends ActionType<GetTermVersionResponse> {

    public static final GetTermVersionAction INSTANCE = new GetTermVersionAction();
    public static final String NAME = "internal:monitor/term";

    private GetTermVersionAction() {
        super(NAME, GetTermVersionResponse::new);
    }
}
