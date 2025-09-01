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

package org.density.action.admin.indices.template.put;

import org.density.action.support.ActionFilters;
import org.density.action.support.clustermanager.AcknowledgedResponse;
import org.density.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.density.cluster.ClusterState;
import org.density.cluster.block.ClusterBlockException;
import org.density.cluster.block.ClusterBlockLevel;
import org.density.cluster.metadata.ComposableIndexTemplate;
import org.density.cluster.metadata.IndexNameExpressionResolver;
import org.density.cluster.metadata.MetadataIndexTemplateService;
import org.density.cluster.metadata.Template;
import org.density.cluster.service.ClusterService;
import org.density.common.compress.CompressedXContent;
import org.density.common.inject.Inject;
import org.density.core.action.ActionListener;
import org.density.core.common.io.stream.StreamInput;
import org.density.index.mapper.MappingTransformerRegistry;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportService;

import java.io.IOException;

import reactor.util.annotation.NonNull;

/**
 * An action for putting a composable index template into the cluster state
 *
 * @density.internal
 */
public class TransportPutComposableIndexTemplateAction extends TransportClusterManagerNodeAction<
    PutComposableIndexTemplateAction.Request,
    AcknowledgedResponse> {

    private final MetadataIndexTemplateService indexTemplateService;
    private final MappingTransformerRegistry mappingTransformerRegistry;

    @Inject
    public TransportPutComposableIndexTemplateAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        MetadataIndexTemplateService indexTemplateService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        MappingTransformerRegistry mappingTransformerRegistry
    ) {
        super(
            PutComposableIndexTemplateAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PutComposableIndexTemplateAction.Request::new,
            indexNameExpressionResolver
        );
        this.indexTemplateService = indexTemplateService;
        this.mappingTransformerRegistry = mappingTransformerRegistry;
    }

    @Override
    protected String executor() {
        // we go async right away
        return ThreadPool.Names.SAME;
    }

    @Override
    protected AcknowledgedResponse read(StreamInput in) throws IOException {
        return new AcknowledgedResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(PutComposableIndexTemplateAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void clusterManagerOperation(
        final PutComposableIndexTemplateAction.Request request,
        final ClusterState state,
        final ActionListener<AcknowledgedResponse> listener
    ) throws IOException {
        final ComposableIndexTemplate indexTemplate = request.indexTemplate();

        final ActionListener<String> mappingTransformListener = ActionListener.wrap(transformedMappings -> {
            if (transformedMappings != null && indexTemplate.template() != null) {
                indexTemplate.template().setMappings(new CompressedXContent(transformedMappings));
            }
            indexTemplateService.putIndexTemplateV2(
                request.cause(),
                request.create(),
                request.name(),
                request.clusterManagerNodeTimeout(),
                indexTemplate,
                listener
            );
        }, listener::onFailure);

        transformMapping(indexTemplate, mappingTransformListener);
    }

    private void transformMapping(
        @NonNull final ComposableIndexTemplate indexTemplate,
        @NonNull final ActionListener<String> mappingTransformListener
    ) {
        final Template template = indexTemplate.template();
        if (template == null || template.mappings() == null) {
            mappingTransformListener.onResponse(null);
        } else {
            mappingTransformerRegistry.applyTransformers(template.mappings().string(), null, mappingTransformListener);
        }
    }
}
