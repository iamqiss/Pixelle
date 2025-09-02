/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.plugin.wlm;

import org.density.action.ActionRequest;
import org.density.action.IndicesRequest;
import org.density.action.search.SearchRequest;
import org.density.action.support.ActionFilter;
import org.density.action.support.ActionFilterChain;
import org.density.core.action.ActionListener;
import org.density.core.action.ActionResponse;
import org.density.plugin.wlm.rule.attribute_extractor.IndicesExtractor;
import org.density.rule.InMemoryRuleProcessingService;
import org.density.tasks.Task;
import org.density.threadpool.ThreadPool;
import org.density.wlm.WorkloadGroupTask;

import java.util.List;
import java.util.Optional;

/**
 * This class is responsible to evaluate and assign the WORKLOAD_GROUP_ID header in ThreadContext
 */
public class AutoTaggingActionFilter implements ActionFilter {
    private final InMemoryRuleProcessingService ruleProcessingService;
    ThreadPool threadPool;

    /**
     * Main constructor
     * @param ruleProcessingService provides access to in memory view of rules
     * @param threadPool to access assign the label
     */
    public AutoTaggingActionFilter(InMemoryRuleProcessingService ruleProcessingService, ThreadPool threadPool) {
        this.ruleProcessingService = ruleProcessingService;
        this.threadPool = threadPool;
    }

    @Override
    public int order() {
        return Integer.MAX_VALUE;
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(
        Task task,
        String action,
        Request request,
        ActionListener<Response> listener,
        ActionFilterChain<Request, Response> chain
    ) {
        final boolean isValidRequest = request instanceof SearchRequest;

        if (!isValidRequest) {
            chain.proceed(task, action, request, listener);
            return;
        }
        Optional<String> label = ruleProcessingService.evaluateLabel(List.of(new IndicesExtractor((IndicesRequest) request)));

        label.ifPresent(s -> threadPool.getThreadContext().putHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, s));
        chain.proceed(task, action, request, listener);
    }
}
