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
 * Transport action to create Rules
 * @density.experimental
 */
public class TransportCreateRuleAction extends TransportAction<CreateRuleRequest, CreateRuleResponse> {
    private final ThreadPool threadPool;
    private final RuleRoutingServiceRegistry ruleRoutingServiceRegistry;
    private final RulePersistenceServiceRegistry rulePersistenceServiceRegistry;

    /**
     * Constructor for TransportCreateRuleAction
     * @param transportService - a {@link TransportService} object
     * @param actionFilters - a {@link ActionFilters} object
     * @param threadPool - a {@link ThreadPool} object
     * @param rulePersistenceServiceRegistry - a {@link RulePersistenceServiceRegistry} object
     * @param ruleRoutingServiceRegistry - a {@link RuleRoutingServiceRegistry} object
     */
    @Inject
    public TransportCreateRuleAction(
        TransportService transportService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        RulePersistenceServiceRegistry rulePersistenceServiceRegistry,
        RuleRoutingServiceRegistry ruleRoutingServiceRegistry
    ) {
        super(CreateRuleAction.NAME, actionFilters, transportService.getTaskManager());
        this.ruleRoutingServiceRegistry = ruleRoutingServiceRegistry;
        this.threadPool = threadPool;
        this.rulePersistenceServiceRegistry = rulePersistenceServiceRegistry;

        transportService.registerRequestHandler(
            CreateRuleAction.NAME,
            ThreadPool.Names.SAME,
            CreateRuleRequest::new,
            new TransportRequestHandler<CreateRuleRequest>() {
                @Override
                public void messageReceived(CreateRuleRequest request, TransportChannel channel, Task task) {
                    executeLocally(request, ActionListener.wrap(response -> {
                        try {
                            channel.sendResponse(response);
                        } catch (IOException e) {
                            logger.error("Failed to send CreateRuleResponse to transport channel", e);
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
    protected void doExecute(Task task, CreateRuleRequest request, ActionListener<CreateRuleResponse> listener) {
        ruleRoutingServiceRegistry.getRuleRoutingService(request.getRule().getFeatureType()).handleCreateRuleRequest(request, listener);
    }

    /**
     * Executes the create rule operation locally on the dedicated rule thread pool.
     * @param request the CreateRuleRequest
     * @param listener listener to handle response or failure
     */
    private void executeLocally(CreateRuleRequest request, ActionListener<CreateRuleResponse> listener) {
        threadPool.executor(RULE_THREAD_POOL_NAME).execute(() -> {
            final RulePersistenceService rulePersistenceService = rulePersistenceServiceRegistry.getRulePersistenceService(
                request.getRule().getFeatureType()
            );
            rulePersistenceService.createRule(request, listener);
        });
    }
}
