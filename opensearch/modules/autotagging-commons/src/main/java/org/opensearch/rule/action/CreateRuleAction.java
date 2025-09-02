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
 * Action type for creating a Rule
 * @density.experimental
 */
public class CreateRuleAction extends ActionType<CreateRuleResponse> {

    /**
     * An instance of CreateRuleAction
     */
    public static final CreateRuleAction INSTANCE = new CreateRuleAction();

    /**
     * Name for CreateRuleAction
     */
    public static final String NAME = "cluster:admin/density/rule/_create";

    /**
     * Default constructor
     */
    private CreateRuleAction() {
        super(NAME, CreateRuleResponse::new);
    }
}
