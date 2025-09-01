/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.search;

import org.density.core.common.breaker.CircuitBreaker;
import org.density.core.common.io.stream.NamedWriteableRegistry;
import org.density.search.SearchPhaseResult;
import org.density.search.query.QuerySearchResult;

import java.util.concurrent.Executor;
import java.util.function.Consumer;

/**
 * Streaming query phase result consumer
 *
 * @density.internal
 */
public class StreamQueryPhaseResultConsumer extends QueryPhaseResultConsumer {

    public StreamQueryPhaseResultConsumer(
        SearchRequest request,
        Executor executor,
        CircuitBreaker circuitBreaker,
        SearchPhaseController controller,
        SearchProgressListener progressListener,
        NamedWriteableRegistry namedWriteableRegistry,
        int expectedResultSize,
        Consumer<Exception> onPartialMergeFailure
    ) {
        super(
            request,
            executor,
            circuitBreaker,
            controller,
            progressListener,
            namedWriteableRegistry,
            expectedResultSize,
            onPartialMergeFailure
        );
    }

    /**
     * For stream search, the minBatchReduceSize is set higher than shard number
     *
     * @param minBatchReduceSize: pass as number of shard
     */
    @Override
    int getBatchReduceSize(int requestBatchedReduceSize, int minBatchReduceSize) {
        return super.getBatchReduceSize(requestBatchedReduceSize, minBatchReduceSize * 10);
    }

    void consumeStreamResult(SearchPhaseResult result, Runnable next) {
        // For streaming, we skip the ArraySearchPhaseResults.consumeResult() call
        // since it doesn't support multiple results from the same shard.
        QuerySearchResult querySearchResult = result.queryResult();
        pendingMerges.consume(querySearchResult, next);
    }
}
