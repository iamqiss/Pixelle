/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.rule;

import org.density.core.action.ActionListener;
import org.density.rule.action.CreateRuleRequest;
import org.density.rule.action.CreateRuleResponse;
import org.density.rule.action.UpdateRuleRequest;
import org.density.rule.action.UpdateRuleResponse;

/**
 * Interface that handles rule routing logic
 * @density.experimental
 */
public interface RuleRoutingService {

    /**
     * Handles a create rule request by routing it to the appropriate node.
     * @param request the create rule request
     * @param listener listener to handle the final response
     */
    void handleCreateRuleRequest(CreateRuleRequest request, ActionListener<CreateRuleResponse> listener);

    /**
     * Handles a update rule request by routing it to the appropriate node.
     * @param request the update rule request
     * @param listener listener to handle the final response
     */
    void handleUpdateRuleRequest(UpdateRuleRequest request, ActionListener<UpdateRuleResponse> listener);
}
