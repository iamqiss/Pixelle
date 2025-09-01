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

import org.density.action.support.ActionFilters;
import org.density.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.density.cluster.ClusterState;
import org.density.cluster.block.ClusterBlockException;
import org.density.cluster.block.ClusterBlockLevel;
import org.density.cluster.metadata.IndexNameExpressionResolver;
import org.density.cluster.metadata.MetadataCreateIndexService;
import org.density.cluster.service.ClusterService;
import org.density.common.inject.Inject;
import org.density.core.action.ActionListener;
import org.density.core.common.io.stream.StreamInput;
import org.density.index.mapper.MappingTransformerRegistry;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportService;

import java.io.IOException;

/**
 * Create index action.
 *
 * @density.internal
 */
public class TransportCreateIndexAction extends TransportClusterManagerNodeAction<CreateIndexRequest, CreateIndexResponse> {

    private final MetadataCreateIndexService createIndexService;
    private final MappingTransformerRegistry mappingTransformerRegistry;

    @Inject
    public TransportCreateIndexAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        MetadataCreateIndexService createIndexService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        MappingTransformerRegistry mappingTransformerRegistry
    ) {
        super(
            CreateIndexAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            CreateIndexRequest::new,
            indexNameExpressionResolver
        );
        this.createIndexService = createIndexService;
        this.mappingTransformerRegistry = mappingTransformerRegistry;
    }

    @Override
    protected String executor() {
        // we go async right away
        return ThreadPool.Names.SAME;
    }

    @Override
    protected CreateIndexResponse read(StreamInput in) throws IOException {
        return new CreateIndexResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(CreateIndexRequest request, ClusterState state) {
        ClusterBlockException clusterBlockException = state.blocks()
            .indexBlockedException(ClusterBlockLevel.METADATA_WRITE, request.index());

        if (clusterBlockException == null) {
            return state.blocks().createIndexBlockedException(ClusterBlockLevel.CREATE_INDEX);
        }
        return clusterBlockException;
    }

    @Override
    protected void clusterManagerOperation(
        final CreateIndexRequest request,
        final ClusterState state,
        final ActionListener<CreateIndexResponse> listener
    ) {
        String cause = request.cause();
        if (cause.length() == 0) {
            cause = "api";
        }

        final String indexName = indexNameExpressionResolver.resolveDateMathExpression(request.index());

        final String finalCause = cause;
        final ActionListener<String> mappingTransformListener = ActionListener.wrap(transformedMappings -> {
            final CreateIndexClusterStateUpdateRequest updateRequest = new CreateIndexClusterStateUpdateRequest(
                finalCause,
                indexName,
                request.index()
            ).ackTimeout(request.timeout())
                .clusterManagerNodeTimeout(request.clusterManagerNodeTimeout())
                .settings(request.settings())
                .mappings(transformedMappings)
                .aliases(request.aliases())
                .context(request.context())
                .waitForActiveShards(request.waitForActiveShards());

            createIndexService.createIndex(
                updateRequest,
                ActionListener.map(
                    listener,
                    response -> new CreateIndexResponse(response.isAcknowledged(), response.isShardsAcknowledged(), indexName)
                )
            );
        }, listener::onFailure);

        mappingTransformerRegistry.applyTransformers(request.mappings(), null, mappingTransformListener);
    }

}
