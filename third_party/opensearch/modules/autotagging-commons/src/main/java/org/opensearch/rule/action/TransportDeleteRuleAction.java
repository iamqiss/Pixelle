/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.rule.action;

import org.density.action.support.ActionFilters;
import org.density.action.support.HandledTransportAction;
import org.density.action.support.clustermanager.AcknowledgedResponse;
import org.density.common.inject.Inject;
import org.density.core.action.ActionListener;
import org.density.rule.RulePersistenceService;
import org.density.rule.RulePersistenceServiceRegistry;
import org.density.tasks.Task;
import org.density.transport.TransportService;

/**
 * Transport action to delete Rules
 * @density.experimental
 */
public class TransportDeleteRuleAction extends HandledTransportAction<DeleteRuleRequest, AcknowledgedResponse> {

    private final RulePersistenceServiceRegistry rulePersistenceServiceRegistry;

    /**
     * Constructor for TransportDeleteRuleAction
     *
     * @param transportService - a {@link TransportService} object
     * @param actionFilters - a {@link ActionFilters} object
     * @param rulePersistenceServiceRegistry - a {@link RulePersistenceServiceRegistry} object
     */
    @Inject
    public TransportDeleteRuleAction(
        TransportService transportService,
        ActionFilters actionFilters,
        RulePersistenceServiceRegistry rulePersistenceServiceRegistry
    ) {
        super(DeleteRuleAction.NAME, transportService, actionFilters, DeleteRuleRequest::new);
        this.rulePersistenceServiceRegistry = rulePersistenceServiceRegistry;
    }

    @Override
    protected void doExecute(Task task, DeleteRuleRequest request, ActionListener<AcknowledgedResponse> listener) {
        RulePersistenceService rulePersistenceService = rulePersistenceServiceRegistry.getRulePersistenceService(request.getFeatureType());
        rulePersistenceService.deleteRule(request, listener);
    }
}
