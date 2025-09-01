/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.rule;

import org.density.action.support.clustermanager.AcknowledgedResponse;
import org.density.core.action.ActionListener;
import org.density.rule.action.CreateRuleRequest;
import org.density.rule.action.CreateRuleResponse;
import org.density.rule.action.DeleteRuleRequest;
import org.density.rule.action.GetRuleRequest;
import org.density.rule.action.GetRuleResponse;
import org.density.rule.action.UpdateRuleRequest;
import org.density.rule.action.UpdateRuleResponse;

/**
 * Interface for a service that handles rule persistence CRUD operations.
 * @density.experimental
 */
public interface RulePersistenceService {

    /**
     * Create rules based on the provided request.
     * @param request The request containing the details for creating the rule.
     * @param listener The listener that will handle the response or failure.
     */
    void createRule(CreateRuleRequest request, ActionListener<CreateRuleResponse> listener);

    /**
     * Get rules based on the provided request.
     * @param request The request containing the details for retrieving the rule.
     * @param listener The listener that will handle the response or failure.
     */
    void getRule(GetRuleRequest request, ActionListener<GetRuleResponse> listener);

    /**
     * Delete a rule based on the provided request.
     * @param request The request containing the ID of the rule to delete.
     * @param listener The listener that will handle the response or failure.
     */
    void deleteRule(DeleteRuleRequest request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Update rule based on the provided request.
     * @param request The request containing the details for updating the rule.
     * @param listener The listener that will handle the response or failure.
     */
    void updateRule(UpdateRuleRequest request, ActionListener<UpdateRuleResponse> listener);
}
