/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.rule.action;

import org.density.action.support.ActionFilters;
import org.density.action.support.TransportAction;
import org.density.common.inject.Inject;
import org.density.core.action.ActionListener;
import org.density.rule.RulePersistenceService;
import org.density.rule.RulePersistenceServiceRegistry;
import org.density.rule.RuleRoutingServiceRegistry;
import org.density.tasks.Task;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportChannel;
import org.density.transport.TransportException;
import org.density.transport.TransportRequestHandler;
import org.density.transport.TransportService;

import java.io.IOException;

import static org.density.rule.RuleFrameworkPlugin.RULE_THREAD_POOL_NAME;

/**
 * Transport action to update Rules
 * @density.experimental
 */
public class TransportUpdateRuleAction extends TransportAction<UpdateRuleRequest, UpdateRuleResponse> {
    private final ThreadPool threadPool;
    private final RuleRoutingServiceRegistry ruleRoutingServiceRegistry;
    private final RulePersistenceServiceRegistry rulePersistenceServiceRegistry;

    /**
     * Constructor for TransportUpdateRuleAction
     * @param transportService - a {@link TransportService} object
     * @param  threadPool - a {@link ThreadPool} object
     * @param actionFilters - a {@link ActionFilters} object
     * @param rulePersistenceServiceRegistry - a {@link RulePersistenceServiceRegistry} object
     * @param ruleRoutingServiceRegistry - a {@link RuleRoutingServiceRegistry} object
     */
    @Inject
    public TransportUpdateRuleAction(
        TransportService transportService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        RulePersistenceServiceRegistry rulePersistenceServiceRegistry,
        RuleRoutingServiceRegistry ruleRoutingServiceRegistry
    ) {
        super(UpdateRuleAction.NAME, actionFilters, transportService.getTaskManager());
        this.rulePersistenceServiceRegistry = rulePersistenceServiceRegistry;
        this.ruleRoutingServiceRegistry = ruleRoutingServiceRegistry;
        this.threadPool = threadPool;

        transportService.registerRequestHandler(
            UpdateRuleAction.NAME,
            ThreadPool.Names.SAME,
            UpdateRuleRequest::new,
            new TransportRequestHandler<UpdateRuleRequest>() {
                @Override
                public void messageReceived(UpdateRuleRequest request, TransportChannel channel, Task task) {
                    executeLocally(request, ActionListener.wrap(response -> {
                        try {
                            channel.sendResponse(response);
                        } catch (IOException e) {
                            logger.error("Failed to send UpdateRuleResponse to transport channel", e);
                            throw new TransportException("Fail to send", e);
                        }
                    }, exception -> {
                        try {
                            channel.sendResponse(exception);
                        } catch (IOException e) {
                            logger.error("Failed to send exception response to transport channel", e);
                            throw new TransportException("Fail to send", e);
                        }
                    }));
                }
            }
        );
    }

    @Override
    protected void doExecute(Task task, UpdateRuleRequest request, ActionListener<UpdateRuleResponse> listener) {
        ruleRoutingServiceRegistry.getRuleRoutingService(request.getFeatureType()).handleUpdateRuleRequest(request, listener);
    }

    /**
     * Executes the update rule operation locally on the dedicated rule thread pool.
     * @param request the UpdateRuleRequest
     * @param listener listener to handle response or failure
     */
    private void executeLocally(UpdateRuleRequest request, ActionListener<UpdateRuleResponse> listener) {
        threadPool.executor(RULE_THREAD_POOL_NAME).execute(() -> {
            final RulePersistenceService rulePersistenceService = rulePersistenceServiceRegistry.getRulePersistenceService(
                request.getFeatureType()
            );
            rulePersistenceService.updateRule(request, listener);
        });
    }
}
