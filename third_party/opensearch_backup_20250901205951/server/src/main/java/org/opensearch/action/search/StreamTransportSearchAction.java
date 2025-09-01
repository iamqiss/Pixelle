/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.search;

import org.density.action.support.ActionFilters;
import org.density.cluster.ClusterState;
import org.density.cluster.metadata.IndexNameExpressionResolver;
import org.density.cluster.routing.GroupShardsIterator;
import org.density.cluster.service.ClusterService;
import org.density.common.Nullable;
import org.density.common.inject.Inject;
import org.density.core.action.ActionListener;
import org.density.core.common.io.stream.NamedWriteableRegistry;
import org.density.core.indices.breaker.CircuitBreakerService;
import org.density.search.SearchPhaseResult;
import org.density.search.SearchService;
import org.density.search.internal.AliasFilter;
import org.density.search.pipeline.SearchPipelineService;
import org.density.tasks.TaskResourceTrackingService;
import org.density.telemetry.metrics.MetricsRegistry;
import org.density.telemetry.tracing.Tracer;
import org.density.threadpool.ThreadPool;
import org.density.transport.StreamTransportService;
import org.density.transport.Transport;
import org.density.transport.client.node.NodeClient;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;

/**
 * Transport search action for streaming search
 * @density.internal
 */
public class StreamTransportSearchAction extends TransportSearchAction {
    @Inject
    public StreamTransportSearchAction(
        NodeClient client,
        ThreadPool threadPool,
        CircuitBreakerService circuitBreakerService,
        @Nullable StreamTransportService transportService,
        SearchService searchService,
        @Nullable StreamSearchTransportService searchTransportService,
        SearchPhaseController searchPhaseController,
        ClusterService clusterService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        NamedWriteableRegistry namedWriteableRegistry,
        SearchPipelineService searchPipelineService,
        MetricsRegistry metricsRegistry,
        SearchRequestOperationsCompositeListenerFactory searchRequestOperationsCompositeListenerFactory,
        Tracer tracer,
        TaskResourceTrackingService taskResourceTrackingService
    ) {
        super(
            client,
            threadPool,
            circuitBreakerService,
            transportService,
            searchService,
            searchTransportService,
            searchPhaseController,
            clusterService,
            actionFilters,
            indexNameExpressionResolver,
            namedWriteableRegistry,
            searchPipelineService,
            metricsRegistry,
            searchRequestOperationsCompositeListenerFactory,
            tracer,
            taskResourceTrackingService
        );
    }

    AbstractSearchAsyncAction<? extends SearchPhaseResult> searchAsyncAction(
        SearchTask task,
        SearchRequest searchRequest,
        Executor executor,
        GroupShardsIterator<SearchShardIterator> shardIterators,
        SearchTimeProvider timeProvider,
        BiFunction<String, String, Transport.Connection> connectionLookup,
        ClusterState clusterState,
        Map<String, AliasFilter> aliasFilter,
        Map<String, Float> concreteIndexBoosts,
        Map<String, Set<String>> indexRoutings,
        ActionListener<SearchResponse> listener,
        boolean preFilter,
        ThreadPool threadPool,
        SearchResponse.Clusters clusters,
        SearchRequestContext searchRequestContext
    ) {
        if (preFilter) {
            throw new IllegalStateException("Search pre-filter is not supported in streaming");
        } else {
            final QueryPhaseResultConsumer queryResultConsumer = searchPhaseController.newStreamSearchPhaseResults(
                executor,
                circuitBreaker,
                task.getProgressListener(),
                searchRequest,
                shardIterators.size(),
                exc -> cancelTask(task, exc)
            );
            AbstractSearchAsyncAction<? extends SearchPhaseResult> searchAsyncAction;
            switch (searchRequest.searchType()) {
                case QUERY_THEN_FETCH:
                    searchAsyncAction = new StreamSearchQueryThenFetchAsyncAction(
                        logger,
                        searchTransportService,
                        connectionLookup,
                        aliasFilter,
                        concreteIndexBoosts,
                        indexRoutings,
                        searchPhaseController,
                        executor,
                        queryResultConsumer,
                        searchRequest,
                        listener,
                        shardIterators,
                        timeProvider,
                        clusterState,
                        task,
                        clusters,
                        searchRequestContext,
                        tracer
                    );
                    break;
                default:
                    throw new IllegalStateException("Unknown search type: [" + searchRequest.searchType() + "]");
            }
            return searchAsyncAction;
        }
    }
}
