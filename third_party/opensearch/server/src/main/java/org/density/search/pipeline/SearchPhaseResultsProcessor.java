/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.search.pipeline;

import org.density.action.search.SearchPhaseContext;
import org.density.action.search.SearchPhaseName;
import org.density.action.search.SearchPhaseResults;
import org.density.search.SearchPhaseResult;
import org.density.search.internal.SearchContext;

/**
 * Creates a processor that runs between Phases of the Search.
 * @density.api
 */
public interface SearchPhaseResultsProcessor extends Processor {

    /**
     * Processes the {@link SearchPhaseResults} obtained from a SearchPhase which will be returned to next
     * SearchPhase.
     * @param searchPhaseResult {@link SearchPhaseResults}
     * @param searchPhaseContext {@link SearchContext}
     * @param <Result> {@link SearchPhaseResult}
     */
    <Result extends SearchPhaseResult> void process(
        final SearchPhaseResults<Result> searchPhaseResult,
        final SearchPhaseContext searchPhaseContext
    );

    /**
     * Processes the {@link SearchPhaseResults} obtained from a SearchPhase which will be returned to next
     * SearchPhase. Receives the {@link PipelineProcessingContext} passed to other processors.
     * @param searchPhaseResult {@link SearchPhaseResults}
     * @param searchPhaseContext {@link SearchContext}
     * @param requestContext {@link PipelineProcessingContext}
     * @param <Result> {@link SearchPhaseResult}
     */
    default <Result extends SearchPhaseResult> void process(
        final SearchPhaseResults<Result> searchPhaseResult,
        final SearchPhaseContext searchPhaseContext,
        final PipelineProcessingContext requestContext
    ) {
        process(searchPhaseResult, searchPhaseContext);
    }

    /**
     * The phase which should have run before, this processor can start executing.
     * @return {@link SearchPhaseName}
     */
    SearchPhaseName getBeforePhase();

    /**
     * The phase which should run after, this processor execution.
     * @return {@link SearchPhaseName}
     */
    SearchPhaseName getAfterPhase();

}
