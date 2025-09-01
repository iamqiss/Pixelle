/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.rule.action;

import org.density.action.ActionType;

/**
 * Action type for getting Rules
 * @density.experimental
 */
public class GetRuleAction extends ActionType<GetRuleResponse> {

    /**
     * An instance of GetRuleAction
     */
    public static final GetRuleAction INSTANCE = new GetRuleAction();

    /**
     * Name for GetRuleAction
     */
    public static final String NAME = "cluster:admin/density/rule/_get";

    /**
     * Default constructor for GetRuleAction
     */
    private GetRuleAction() {
        super(NAME, GetRuleResponse::new);
    }
}
