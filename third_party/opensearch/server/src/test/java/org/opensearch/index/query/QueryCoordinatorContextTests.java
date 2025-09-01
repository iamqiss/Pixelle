/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.index.query;

import org.density.action.IndicesRequest;
import org.density.action.search.SearchRequest;
import org.density.cluster.metadata.IndexNameExpressionResolver;
import org.density.cluster.service.ClusterService;
import org.density.common.settings.Settings;
import org.density.common.util.concurrent.DensityExecutors;
import org.density.common.util.concurrent.ThreadContext;
import org.density.plugins.SearchPipelinePlugin;
import org.density.search.pipeline.PipelinedRequest;
import org.density.search.pipeline.Processor;
import org.density.search.pipeline.SearchPhaseResultsProcessor;
import org.density.search.pipeline.SearchPipelineService;
import org.density.search.pipeline.SearchRequestProcessor;
import org.density.search.pipeline.SearchResponseProcessor;
import org.density.test.DensityTestCase;
import org.density.threadpool.ThreadPool;
import org.density.transport.client.Client;
import org.junit.Before;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class QueryCoordinatorContextTests extends DensityTestCase {

    private IndexNameExpressionResolver indexNameExpressionResolver;

    @Before
    public void setup() {
        indexNameExpressionResolver = new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY));
    }

    public void testGetContextVariables_whenPipelinedSearchRequest_thenReturnVariables() {
        final PipelinedRequest searchRequest = createDummyPipelinedRequest();
        searchRequest.getPipelineProcessingContext().setAttribute("key", "value");

        final QueryCoordinatorContext queryCoordinatorContext = new QueryCoordinatorContext(mock(QueryRewriteContext.class), searchRequest);

        assertEquals(Map.of("key", "value"), queryCoordinatorContext.getContextVariables());
    }

    private PipelinedRequest createDummyPipelinedRequest() {
        final Client client = mock(Client.class);
        final ThreadPool threadPool = mock(ThreadPool.class);
        final ExecutorService executorService = DensityExecutors.newDirectExecutorService();
        when(threadPool.generic()).thenReturn(executorService);
        when(threadPool.executor(anyString())).thenReturn(executorService);
        final SearchPipelineService searchPipelineService = new SearchPipelineService(
            mock(ClusterService.class),
            threadPool,
            null,
            null,
            null,
            null,
            this.writableRegistry(),
            Collections.singletonList(new SearchPipelinePlugin() {
                @Override
                public Map<String, Processor.Factory<SearchRequestProcessor>> getRequestProcessors(Parameters parameters) {
                    return Collections.emptyMap();
                }

                @Override
                public Map<String, Processor.Factory<SearchResponseProcessor>> getResponseProcessors(Parameters parameters) {
                    return Collections.emptyMap();
                }

                @Override
                public Map<String, Processor.Factory<SearchPhaseResultsProcessor>> getSearchPhaseResultsProcessors(Parameters parameters) {
                    return Collections.emptyMap();
                }

            }),
            client
        );
        final SearchRequest searchRequest = new SearchRequest();
        return searchPipelineService.resolvePipeline(searchRequest, indexNameExpressionResolver);
    }

    public void testGetContextVariables_whenNotPipelinedSearchRequest_thenReturnEmpty() {
        final IndicesRequest searchRequest = mock(IndicesRequest.class);

        final QueryCoordinatorContext queryCoordinatorContext = new QueryCoordinatorContext(mock(QueryRewriteContext.class), searchRequest);

        assertTrue(queryCoordinatorContext.getContextVariables().isEmpty());
    }

    public void testGetSearchRequest() {
        final IndicesRequest searchRequest = mock(IndicesRequest.class);

        final QueryCoordinatorContext queryCoordinatorContext = new QueryCoordinatorContext(mock(QueryRewriteContext.class), searchRequest);

        assertEquals(searchRequest, queryCoordinatorContext.getSearchRequest());
    }
}
