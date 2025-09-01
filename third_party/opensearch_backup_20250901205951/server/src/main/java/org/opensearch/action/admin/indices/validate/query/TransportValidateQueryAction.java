/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright Density Contributors. See
 * GitHub history for details.
 */

package org.density.action.admin.indices.validate.query;

import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.density.action.support.ActionFilters;
import org.density.action.support.broadcast.BroadcastShardOperationFailedException;
import org.density.action.support.broadcast.TransportBroadcastAction;
import org.density.cluster.ClusterState;
import org.density.cluster.block.ClusterBlockException;
import org.density.cluster.block.ClusterBlockLevel;
import org.density.cluster.metadata.IndexNameExpressionResolver;
import org.density.cluster.routing.GroupShardsIterator;
import org.density.cluster.routing.ShardRouting;
import org.density.cluster.service.ClusterService;
import org.density.common.Randomness;
import org.density.common.inject.Inject;
import org.density.common.lease.Releasables;
import org.density.core.action.ActionListener;
import org.density.core.action.support.DefaultShardOperationFailedException;
import org.density.core.common.ParsingException;
import org.density.core.common.io.stream.StreamInput;
import org.density.index.IndexNotFoundException;
import org.density.index.query.ParsedQuery;
import org.density.index.query.QueryShardException;
import org.density.index.query.Rewriteable;
import org.density.indices.IndexClosedException;
import org.density.search.SearchService;
import org.density.search.internal.AliasFilter;
import org.density.search.internal.SearchContext;
import org.density.search.internal.ShardSearchRequest;
import org.density.tasks.Task;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.LongSupplier;

/**
 * Transport Action to Validate a Query
 *
 * @density.internal
 */
public class TransportValidateQueryAction extends TransportBroadcastAction<
    ValidateQueryRequest,
    ValidateQueryResponse,
    ShardValidateQueryRequest,
    ShardValidateQueryResponse> {

    private final SearchService searchService;

    @Inject
    public TransportValidateQueryAction(
        ClusterService clusterService,
        TransportService transportService,
        SearchService searchService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            ValidateQueryAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            ValidateQueryRequest::new,
            ShardValidateQueryRequest::new,
            ThreadPool.Names.SEARCH
        );
        this.searchService = searchService;
    }

    @Override
    protected void doExecute(Task task, ValidateQueryRequest request, ActionListener<ValidateQueryResponse> listener) {
        request.nowInMillis = System.currentTimeMillis();
        LongSupplier timeProvider = () -> request.nowInMillis;
        ActionListener<org.density.index.query.QueryBuilder> rewriteListener = ActionListener.wrap(rewrittenQuery -> {
            request.query(rewrittenQuery);
            super.doExecute(task, request, listener);
        }, ex -> {
            if (ex instanceof IndexNotFoundException || ex instanceof IndexClosedException) {
                listener.onFailure(ex);
                return;
            }
            List<QueryExplanation> explanations = new ArrayList<>();
            explanations.add(new QueryExplanation(null, QueryExplanation.RANDOM_SHARD, false, null, ex.getMessage()));
            listener.onResponse(
                new ValidateQueryResponse(
                    false,
                    explanations,
                    // totalShards is documented as "the total shards this request ran against",
                    // which is 0 since the failure is happening on the coordinating node.
                    0,
                    0,
                    0,
                    null
                )
            );
        });
        if (request.query() == null) {
            rewriteListener.onResponse(request.query());
        } else {
            Rewriteable.rewriteAndFetch(request.query(), searchService.getValidationRewriteContext(timeProvider, request), rewriteListener);
        }
    }

    @Override
    protected ShardValidateQueryRequest newShardRequest(int numShards, ShardRouting shard, ValidateQueryRequest request) {
        final ClusterState clusterState = clusterService.state();
        final Set<String> indicesAndAliases = indexNameExpressionResolver.resolveExpressions(clusterState, request.indices());
        final AliasFilter aliasFilter = searchService.buildAliasFilter(clusterState, shard.getIndexName(), indicesAndAliases);
        return new ShardValidateQueryRequest(shard.shardId(), aliasFilter, request);
    }

    @Override
    protected ShardValidateQueryResponse readShardResponse(StreamInput in) throws IOException {
        return new ShardValidateQueryResponse(in);
    }

    @Override
    protected GroupShardsIterator shards(ClusterState clusterState, ValidateQueryRequest request, String[] concreteIndices) {
        final String routing;
        if (request.allShards()) {
            routing = null;
        } else {
            // Random routing to limit request to a single shard
            routing = Integer.toString(Randomness.get().nextInt(1000));
        }
        Map<String, Set<String>> routingMap = indexNameExpressionResolver.resolveSearchRouting(clusterState, routing, request.indices());
        return clusterService.operationRouting().searchShards(clusterState, concreteIndices, routingMap, "_local");
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, ValidateQueryRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, ValidateQueryRequest countRequest, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.READ, concreteIndices);
    }

    @Override
    protected ValidateQueryResponse newResponse(
        ValidateQueryRequest request,
        AtomicReferenceArray shardsResponses,
        ClusterState clusterState
    ) {
        int successfulShards = 0;
        int failedShards = 0;
        boolean valid = true;
        List<DefaultShardOperationFailedException> shardFailures = null;
        List<QueryExplanation> queryExplanations = null;
        for (int i = 0; i < shardsResponses.length(); i++) {
            Object shardResponse = shardsResponses.get(i);
            if (shardResponse == null) {
                // simply ignore non active shards
            } else if (shardResponse instanceof BroadcastShardOperationFailedException) {
                failedShards++;
                if (shardFailures == null) {
                    shardFailures = new ArrayList<>();
                }
                shardFailures.add(new DefaultShardOperationFailedException((BroadcastShardOperationFailedException) shardResponse));
            } else {
                ShardValidateQueryResponse validateQueryResponse = (ShardValidateQueryResponse) shardResponse;
                valid = valid && validateQueryResponse.isValid();
                if (request.explain() || request.rewrite() || request.allShards()) {
                    if (queryExplanations == null) {
                        queryExplanations = new ArrayList<>();
                    }
                    queryExplanations.add(
                        new QueryExplanation(
                            validateQueryResponse.getIndex(),
                            request.allShards() ? validateQueryResponse.getShardId().getId() : QueryExplanation.RANDOM_SHARD,
                            validateQueryResponse.isValid(),
                            validateQueryResponse.getExplanation(),
                            validateQueryResponse.getError()
                        )
                    );
                }
                successfulShards++;
            }
        }
        return new ValidateQueryResponse(valid, queryExplanations, shardsResponses.length(), successfulShards, failedShards, shardFailures);
    }

    @Override
    protected ShardValidateQueryResponse shardOperation(ShardValidateQueryRequest request, Task task) throws IOException {
        boolean valid;
        String explanation = null;
        String error = null;
        ShardSearchRequest shardSearchLocalRequest = new ShardSearchRequest(
            request.shardId(),
            request.nowInMillis(),
            request.filteringAliases()
        );
        SearchContext searchContext = searchService.createValidationContext(shardSearchLocalRequest, SearchService.NO_TIMEOUT);
        try {
            ParsedQuery parsedQuery = searchContext.getQueryShardContext().toQuery(request.query());
            searchContext.parsedQuery(parsedQuery);
            searchContext.preProcess(request.rewrite());
            valid = true;
            explanation = explain(searchContext, request.rewrite());
        } catch (QueryShardException | ParsingException e) {
            valid = false;
            error = e.getDetailedMessage();
        } catch (AssertionError e) {
            valid = false;
            error = e.getMessage();
        } finally {
            Releasables.close(searchContext);
        }

        return new ShardValidateQueryResponse(request.shardId(), valid, explanation, error);
    }

    private String explain(SearchContext context, boolean rewritten) {
        Query query = context.query();
        if (rewritten && query instanceof MatchNoDocsQuery) {
            return context.parsedQuery().query().toString();
        } else {
            return query.toString();
        }
    }
}
