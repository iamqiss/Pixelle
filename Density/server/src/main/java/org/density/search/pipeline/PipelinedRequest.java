/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.search.pipeline;

import org.density.action.search.SearchPhaseContext;
import org.density.action.search.SearchPhaseResults;
import org.density.action.search.SearchRequest;
import org.density.action.search.SearchResponse;
import org.density.core.action.ActionListener;
import org.density.search.SearchPhaseResult;

import java.util.List;

/**
 * Groups a search pipeline based on a request and the request after being transformed by the pipeline.
 *
 * @density.internal
 */
public final class PipelinedRequest extends SearchRequest {
    private final Pipeline pipeline;
    private final PipelineProcessingContext requestContext;

    PipelinedRequest(Pipeline pipeline, SearchRequest transformedRequest, PipelineProcessingContext requestContext) {
        super(transformedRequest);
        this.pipeline = pipeline;
        this.requestContext = requestContext;
    }

    public void transformRequest(ActionListener<SearchRequest> requestListener) {
        pipeline.transformRequest(this, requestListener, requestContext);
    }

    public ActionListener<SearchResponse> transformResponseListener(ActionListener<SearchResponse> responseListener) {
        return pipeline.transformResponseListener(this, ActionListener.wrap(response -> {
            // Extract processor execution details
            List<ProcessorExecutionDetail> details = requestContext.getProcessorExecutionDetails();
            // Add details to the response's InternalResponse if available
            if (!details.isEmpty() && response.getInternalResponse() != null) {
                response.getInternalResponse().getProcessorResult().addAll(details);
            }
            responseListener.onResponse(response);
        }, responseListener::onFailure), requestContext);
    }

    public <Result extends SearchPhaseResult> void transformSearchPhaseResults(
        final SearchPhaseResults<Result> searchPhaseResult,
        final SearchPhaseContext searchPhaseContext,
        final String currentPhase,
        final String nextPhase
    ) {
        pipeline.runSearchPhaseResultsTransformer(searchPhaseResult, searchPhaseContext, currentPhase, nextPhase, requestContext);
    }

    // Visible for testing
    Pipeline getPipeline() {
        return pipeline;
    }

    public PipelineProcessingContext getPipelineProcessingContext() {
        return requestContext;
    }
}
