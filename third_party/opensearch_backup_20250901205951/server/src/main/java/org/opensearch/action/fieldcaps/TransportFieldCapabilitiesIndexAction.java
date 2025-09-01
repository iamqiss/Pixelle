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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.density.action.fieldcaps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.density.action.ActionRunnable;
import org.density.action.ActionType;
import org.density.action.NoShardAvailableActionException;
import org.density.action.support.ActionFilters;
import org.density.action.support.ChannelActionListener;
import org.density.action.support.HandledTransportAction;
import org.density.cluster.ClusterState;
import org.density.cluster.block.ClusterBlockException;
import org.density.cluster.block.ClusterBlockLevel;
import org.density.cluster.metadata.IndexNameExpressionResolver;
import org.density.cluster.node.DiscoveryNode;
import org.density.cluster.node.DiscoveryNodes;
import org.density.cluster.routing.FailAwareWeightedRouting;
import org.density.cluster.routing.GroupShardsIterator;
import org.density.cluster.routing.ShardIterator;
import org.density.cluster.routing.ShardRouting;
import org.density.cluster.service.ClusterService;
import org.density.common.Nullable;
import org.density.common.inject.Inject;
import org.density.core.action.ActionListener;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.logging.LoggerMessageFormat;
import org.density.core.index.shard.ShardId;
import org.density.index.mapper.MappedFieldType;
import org.density.index.mapper.MapperService;
import org.density.index.mapper.ObjectMapper;
import org.density.index.query.MatchAllQueryBuilder;
import org.density.indices.IndicesService;
import org.density.search.SearchService;
import org.density.search.builder.SearchSourceBuilder;
import org.density.search.internal.AliasFilter;
import org.density.search.internal.ShardSearchRequest;
import org.density.tasks.Task;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportChannel;
import org.density.transport.TransportException;
import org.density.transport.TransportRequestHandler;
import org.density.transport.TransportResponseHandler;
import org.density.transport.TransportService;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.Predicate;

import static org.density.action.support.TransportActions.isShardNotAvailableException;

/**
 * Transport action for field capabilities request in an index
 *
 * @density.internal
 */
public class TransportFieldCapabilitiesIndexAction extends HandledTransportAction<
    FieldCapabilitiesIndexRequest,
    FieldCapabilitiesIndexResponse> {

    private static final Logger logger = LogManager.getLogger(TransportFieldCapabilitiesIndexAction.class);

    private static final String ACTION_NAME = FieldCapabilitiesAction.NAME + "[index]";
    private static final String ACTION_SHARD_NAME = ACTION_NAME + "[s]";
    public static final ActionType<FieldCapabilitiesIndexResponse> TYPE = new ActionType<>(
        ACTION_NAME,
        FieldCapabilitiesIndexResponse::new
    );

    private final ClusterService clusterService;
    private final TransportService transportService;
    private final SearchService searchService;
    private final IndicesService indicesService;
    private final Executor executor;

    @Inject
    public TransportFieldCapabilitiesIndexAction(
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        SearchService searchService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(ACTION_NAME, transportService, actionFilters, FieldCapabilitiesIndexRequest::new);
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.searchService = searchService;
        this.indicesService = indicesService;
        this.executor = threadPool.executor(ThreadPool.Names.MANAGEMENT);
        transportService.registerRequestHandler(
            ACTION_SHARD_NAME,
            ThreadPool.Names.SAME,
            FieldCapabilitiesIndexRequest::new,
            new ShardTransportHandler()
        );
    }

    @Override
    protected void doExecute(Task task, FieldCapabilitiesIndexRequest request, ActionListener<FieldCapabilitiesIndexResponse> listener) {
        new AsyncShardsAction(request, listener).start();
    }

    private FieldCapabilitiesIndexResponse shardOperation(final FieldCapabilitiesIndexRequest request) throws IOException {
        if (canMatchShard(request) == false) {
            return new FieldCapabilitiesIndexResponse(request.index(), Collections.emptyMap(), false);
        }
        ShardId shardId = request.shardId();
        MapperService mapperService = indicesService.indexServiceSafe(shardId.getIndex()).mapperService();
        Set<String> fieldNames = new HashSet<>();
        for (String field : request.fields()) {
            fieldNames.addAll(mapperService.simpleMatchToFullName(field));
        }
        Predicate<String> fieldPredicate = indicesService.getFieldFilter().apply(shardId.getIndexName());
        Map<String, IndexFieldCapabilities> responseMap = new HashMap<>();
        for (String field : fieldNames) {
            MappedFieldType ft = mapperService.fieldType(field);
            if (ft != null) {
                if (indicesService.isMetadataField(field) || fieldPredicate.test(ft.name())) {
                    IndexFieldCapabilities fieldCap = new IndexFieldCapabilities(
                        field,
                        ft.familyTypeName(),
                        ft.isSearchable(),
                        ft.isAggregatable(),
                        ft.meta()
                    );
                    responseMap.put(field, fieldCap);
                } else {
                    continue;
                }
                // add nested and object fields
                int dotIndex = ft.name().lastIndexOf('.');
                while (dotIndex > -1) {
                    String parentField = ft.name().substring(0, dotIndex);
                    if (responseMap.containsKey(parentField)) {
                        // we added this path on another field already
                        break;
                    }
                    // checks if the parent field contains sub-fields
                    if (mapperService.fieldType(parentField) == null) {
                        // no field type, it must be an object field
                        ObjectMapper mapper = mapperService.getObjectMapper(parentField);
                        String type = mapper.nested().isNested() ? "nested" : "object";
                        IndexFieldCapabilities fieldCap = new IndexFieldCapabilities(
                            parentField,
                            type,
                            false,
                            false,
                            Collections.emptyMap()
                        );
                        responseMap.put(parentField, fieldCap);
                    }
                    dotIndex = parentField.lastIndexOf('.');
                }
            }
        }
        return new FieldCapabilitiesIndexResponse(request.index(), responseMap, true);
    }

    private boolean canMatchShard(FieldCapabilitiesIndexRequest req) throws IOException {
        if (req.indexFilter() == null || req.indexFilter() instanceof MatchAllQueryBuilder) {
            return true;
        }
        assert req.nowInMillis() != 0L;
        ShardSearchRequest searchRequest = new ShardSearchRequest(req.shardId(), req.nowInMillis(), AliasFilter.EMPTY);
        searchRequest.source(new SearchSourceBuilder().query(req.indexFilter()));
        return searchService.canMatch(searchRequest).canMatch();
    }

    private ClusterBlockException checkGlobalBlock(ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.READ);
    }

    private ClusterBlockException checkRequestBlock(ClusterState state, String concreteIndex) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.READ, concreteIndex);
    }

    /**
     * An action that executes on each shard sequentially until it finds one that can match the provided
     * {@link FieldCapabilitiesIndexRequest#indexFilter()}. In which case the shard is used
     * to create the final {@link FieldCapabilitiesIndexResponse}.
     *
     * @density.internal
     */
    class AsyncShardsAction {
        private final FieldCapabilitiesIndexRequest request;
        private final DiscoveryNodes nodes;
        private final ActionListener<FieldCapabilitiesIndexResponse> listener;
        private final GroupShardsIterator<ShardIterator> shardsIt;

        private volatile int shardIndex = 0;

        private AsyncShardsAction(FieldCapabilitiesIndexRequest request, ActionListener<FieldCapabilitiesIndexResponse> listener) {
            this.listener = listener;

            ClusterState clusterState = clusterService.state();
            if (logger.isTraceEnabled()) {
                logger.trace("executing [{}] based on cluster state version [{}]", request, clusterState.version());
            }
            nodes = clusterState.nodes();
            ClusterBlockException blockException = checkGlobalBlock(clusterState);
            if (blockException != null) {
                throw blockException;
            }

            this.request = request;
            blockException = checkRequestBlock(clusterState, request.index());
            if (blockException != null) {
                throw blockException;
            }

            shardsIt = clusterService.operationRouting().searchShards(clusterService.state(), new String[] { request.index() }, null, null);
        }

        public void start() {
            tryNext(null, true);
        }

        private void onFailure(ShardRouting shardRouting, Exception e) {
            if (e != null) {
                logger.trace(() -> new ParameterizedMessage("{}: failed to execute [{}]", shardRouting, request), e);
            }
            tryNext(e, false);
        }

        private ShardRouting nextRoutingOrNull(Exception failure) {
            if (shardsIt.size() == 0 || shardIndex >= shardsIt.size()) {
                return null;
            }
            ShardRouting next = FailAwareWeightedRouting.getInstance()
                .findNext(shardsIt.get(shardIndex), clusterService.state(), failure, this::moveToNextShard);

            if (next != null) {
                return next;
            }
            moveToNextShard();
            return nextRoutingOrNull(failure);
        }

        private void moveToNextShard() {
            ++shardIndex;
        }

        private void tryNext(@Nullable final Exception lastFailure, boolean canMatchShard) {
            ShardRouting shardRouting = nextRoutingOrNull(lastFailure);
            if (shardRouting == null) {
                if (canMatchShard == false) {
                    listener.onResponse(new FieldCapabilitiesIndexResponse(request.index(), Collections.emptyMap(), false));
                } else {
                    if (lastFailure == null || isShardNotAvailableException(lastFailure)) {
                        listener.onFailure(
                            new NoShardAvailableActionException(
                                null,
                                LoggerMessageFormat.format("No shard available for [{}]", request),
                                lastFailure
                            )
                        );
                    } else {
                        logger.debug(() -> new ParameterizedMessage("{}: failed to execute [{}]", null, request), lastFailure);
                        listener.onFailure(lastFailure);
                    }
                }
                return;
            }
            DiscoveryNode node = nodes.get(shardRouting.currentNodeId());
            if (node == null) {
                onFailure(shardRouting, new NoShardAvailableActionException(shardRouting.shardId()));
            } else {
                request.shardId(shardRouting.shardId());
                if (logger.isTraceEnabled()) {
                    logger.trace("sending request [{}] on node [{}]", request, node);
                }
                transportService.sendRequest(
                    node,
                    ACTION_SHARD_NAME,
                    request,
                    new TransportResponseHandler<FieldCapabilitiesIndexResponse>() {

                        @Override
                        public FieldCapabilitiesIndexResponse read(StreamInput in) throws IOException {
                            return new FieldCapabilitiesIndexResponse(in);
                        }

                        @Override
                        public String executor() {
                            return ThreadPool.Names.SAME;
                        }

                        @Override
                        public void handleResponse(final FieldCapabilitiesIndexResponse response) {
                            if (response.canMatch()) {
                                listener.onResponse(response);
                            } else {
                                moveToNextShard();
                                tryNext(null, false);
                            }
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            onFailure(shardRouting, exp);
                        }
                    }
                );
            }
        }
    }

    /**
     * Shard transport handler for field capabilities index action
     *
     * @density.internal
     */
    private class ShardTransportHandler implements TransportRequestHandler<FieldCapabilitiesIndexRequest> {
        @Override
        public void messageReceived(final FieldCapabilitiesIndexRequest request, final TransportChannel channel, Task task)
            throws Exception {
            if (logger.isTraceEnabled()) {
                logger.trace("executing [{}]", request);
            }
            ActionListener<FieldCapabilitiesIndexResponse> listener = new ChannelActionListener<>(channel, ACTION_SHARD_NAME, request);
            executor.execute(ActionRunnable.supply(listener, () -> shardOperation(request)));
        }
    }
}
