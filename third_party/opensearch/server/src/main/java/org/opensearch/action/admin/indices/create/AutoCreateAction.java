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

package org.density.action.admin.indices.create;

import org.density.action.ActionType;
import org.density.action.support.ActionFilters;
import org.density.action.support.ActiveShardCount;
import org.density.action.support.ActiveShardsObserver;
import org.density.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.density.cluster.AckedClusterStateUpdateTask;
import org.density.cluster.ClusterState;
import org.density.cluster.ack.ClusterStateUpdateResponse;
import org.density.cluster.block.ClusterBlockException;
import org.density.cluster.block.ClusterBlockLevel;
import org.density.cluster.metadata.ComposableIndexTemplate;
import org.density.cluster.metadata.ComposableIndexTemplate.DataStreamTemplate;
import org.density.cluster.metadata.IndexNameExpressionResolver;
import org.density.cluster.metadata.Metadata;
import org.density.cluster.metadata.MetadataCreateDataStreamService;
import org.density.cluster.metadata.MetadataCreateDataStreamService.CreateDataStreamClusterStateUpdateRequest;
import org.density.cluster.metadata.MetadataCreateIndexService;
import org.density.cluster.metadata.MetadataIndexTemplateService;
import org.density.cluster.service.ClusterManagerTaskThrottler;
import org.density.cluster.service.ClusterService;
import org.density.common.Priority;
import org.density.common.inject.Inject;
import org.density.core.action.ActionListener;
import org.density.core.common.io.stream.StreamInput;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportService;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import static org.density.cluster.service.ClusterManagerTask.AUTO_CREATE;

/**
 * Api that auto creates an index or data stream that originate from requests that write into an index that doesn't yet exist.
 *
 * @density.internal
 */
public final class AutoCreateAction extends ActionType<CreateIndexResponse> {

    public static final AutoCreateAction INSTANCE = new AutoCreateAction();
    public static final String NAME = "indices:admin/auto_create";

    private AutoCreateAction() {
        super(NAME, CreateIndexResponse::new);
    }

    /**
     * Transport Action for Auto Create
     *
     * @density.internal
     */
    public static final class TransportAction extends TransportClusterManagerNodeAction<CreateIndexRequest, CreateIndexResponse> {

        private final ActiveShardsObserver activeShardsObserver;
        private final MetadataCreateIndexService createIndexService;
        private final MetadataCreateDataStreamService metadataCreateDataStreamService;
        private final ClusterManagerTaskThrottler.ThrottlingKey autoCreateTaskKey;

        @Inject
        public TransportAction(
            TransportService transportService,
            ClusterService clusterService,
            ThreadPool threadPool,
            ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver,
            MetadataCreateIndexService createIndexService,
            MetadataCreateDataStreamService metadataCreateDataStreamService
        ) {
            super(NAME, transportService, clusterService, threadPool, actionFilters, CreateIndexRequest::new, indexNameExpressionResolver);
            this.activeShardsObserver = new ActiveShardsObserver(clusterService, threadPool);
            this.createIndexService = createIndexService;
            this.metadataCreateDataStreamService = metadataCreateDataStreamService;

            // Task is onboarded for throttling, it will get retried from associated TransportClusterManagerNodeAction.
            autoCreateTaskKey = clusterService.registerClusterManagerTask(AUTO_CREATE, true);
        }

        @Override
        protected String executor() {
            return ThreadPool.Names.SAME;
        }

        @Override
        protected CreateIndexResponse read(StreamInput in) throws IOException {
            return new CreateIndexResponse(in);
        }

        @Override
        protected void clusterManagerOperation(
            CreateIndexRequest request,
            ClusterState state,
            ActionListener<CreateIndexResponse> finalListener
        ) {
            AtomicReference<String> indexNameRef = new AtomicReference<>();
            ActionListener<ClusterStateUpdateResponse> listener = ActionListener.wrap(response -> {
                String indexName = indexNameRef.get();
                assert indexName != null;
                if (response.isAcknowledged()) {
                    activeShardsObserver.waitForActiveShards(
                        new String[] { indexName },
                        ActiveShardCount.DEFAULT,
                        request.timeout(),
                        shardsAcked -> {
                            finalListener.onResponse(new CreateIndexResponse(true, shardsAcked, indexName));
                        },
                        finalListener::onFailure
                    );
                } else {
                    finalListener.onResponse(new CreateIndexResponse(false, false, indexName));
                }
            }, finalListener::onFailure);
            clusterService.submitStateUpdateTask(
                "auto create [" + request.index() + "]",
                new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(Priority.URGENT, request, listener) {

                    @Override
                    protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                        return new ClusterStateUpdateResponse(acknowledged);
                    }

                    @Override
                    public ClusterManagerTaskThrottler.ThrottlingKey getClusterManagerThrottlingKey() {
                        return autoCreateTaskKey;
                    }

                    @Override
                    public ClusterState execute(ClusterState currentState) throws Exception {
                        DataStreamTemplate dataStreamTemplate = resolveAutoCreateDataStream(request, currentState.metadata());
                        if (dataStreamTemplate != null) {
                            CreateDataStreamClusterStateUpdateRequest createRequest = new CreateDataStreamClusterStateUpdateRequest(
                                request.index(),
                                request.clusterManagerNodeTimeout(),
                                request.timeout()
                            );
                            ClusterState clusterState = metadataCreateDataStreamService.createDataStream(createRequest, currentState);
                            indexNameRef.set(clusterState.metadata().dataStreams().get(request.index()).getIndices().get(0).getName());
                            return clusterState;
                        } else {
                            String indexName = indexNameExpressionResolver.resolveDateMathExpression(request.index());
                            indexNameRef.set(indexName);
                            CreateIndexClusterStateUpdateRequest updateRequest = new CreateIndexClusterStateUpdateRequest(
                                request.cause(),
                                indexName,
                                request.index()
                            ).ackTimeout(request.timeout()).clusterManagerNodeTimeout(request.clusterManagerNodeTimeout());
                            return createIndexService.applyCreateIndexRequest(currentState, updateRequest, false);
                        }
                    }
                }
            );
        }

        @Override
        protected ClusterBlockException checkBlock(CreateIndexRequest request, ClusterState state) {
            return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA_WRITE, request.index());
        }
    }

    static DataStreamTemplate resolveAutoCreateDataStream(CreateIndexRequest request, Metadata metadata) {
        String v2Template = MetadataIndexTemplateService.findV2Template(metadata, request.index(), false);
        if (v2Template != null) {
            ComposableIndexTemplate composableIndexTemplate = metadata.templatesV2().get(v2Template);
            if (composableIndexTemplate.getDataStreamTemplate() != null) {
                return composableIndexTemplate.getDataStreamTemplate();
            }
        }

        return null;
    }

}
