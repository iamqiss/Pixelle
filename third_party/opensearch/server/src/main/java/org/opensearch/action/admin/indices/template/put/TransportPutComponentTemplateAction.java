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
import org.density.cluster.metadata.ComponentTemplate;
import org.density.cluster.metadata.IndexMetadata;
import org.density.cluster.metadata.IndexNameExpressionResolver;
import org.density.cluster.metadata.MetadataIndexTemplateService;
import org.density.cluster.metadata.Template;
import org.density.cluster.service.ClusterService;
import org.density.common.compress.CompressedXContent;
import org.density.common.inject.Inject;
import org.density.common.settings.IndexScopedSettings;
import org.density.common.settings.Settings;
import org.density.core.action.ActionListener;
import org.density.core.common.io.stream.StreamInput;
import org.density.index.mapper.MappingTransformerRegistry;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportService;

import java.io.IOException;

import reactor.util.annotation.NonNull;

/**
 * An action for putting a single component template into the cluster state
 *
 * @density.internal
 */
public class TransportPutComponentTemplateAction extends TransportClusterManagerNodeAction<
    PutComponentTemplateAction.Request,
    AcknowledgedResponse> {

    private final MetadataIndexTemplateService indexTemplateService;
    private final IndexScopedSettings indexScopedSettings;
    private final MappingTransformerRegistry mappingTransformerRegistry;

    @Inject
    public TransportPutComponentTemplateAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        MetadataIndexTemplateService indexTemplateService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        IndexScopedSettings indexScopedSettings,
        MappingTransformerRegistry mappingTransformerRegistry
    ) {
        super(
            PutComponentTemplateAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PutComponentTemplateAction.Request::new,
            indexNameExpressionResolver
        );
        this.indexTemplateService = indexTemplateService;
        this.indexScopedSettings = indexScopedSettings;
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
    protected ClusterBlockException checkBlock(PutComponentTemplateAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void clusterManagerOperation(
        final PutComponentTemplateAction.Request request,
        final ClusterState state,
        final ActionListener<AcknowledgedResponse> listener
    ) {
        ComponentTemplate componentTemplate = request.componentTemplate();
        Template template = componentTemplate.template();
        // Normalize the index settings if necessary
        if (template.settings() != null) {
            Settings.Builder builder = Settings.builder().put(template.settings()).normalizePrefix(IndexMetadata.INDEX_SETTING_PREFIX);
            Settings settings = builder.build();
            indexScopedSettings.validate(settings, true);
            template = new Template(settings, template.mappings(), template.aliases());
            componentTemplate = new ComponentTemplate(template, componentTemplate.version(), componentTemplate.metadata());
        }

        final ActionListener<String> mappingTransformListener = getMappingTransformListener(request, listener, componentTemplate);

        transformMapping(template, mappingTransformListener);
    }

    private ActionListener<String> getMappingTransformListener(
        @NonNull final PutComponentTemplateAction.Request request,
        @NonNull final ActionListener<AcknowledgedResponse> listener,
        @NonNull final ComponentTemplate componentTemplate
    ) {
        return ActionListener.wrap(transformedMappings -> {
            if (transformedMappings != null && componentTemplate.template() != null) {
                componentTemplate.template().setMappings(new CompressedXContent(transformedMappings));
            }
            indexTemplateService.putComponentTemplate(
                request.cause(),
                request.create(),
                request.name(),
                request.clusterManagerNodeTimeout(),
                componentTemplate,
                listener
            );
        }, listener::onFailure);
    }

    private void transformMapping(final Template template, @NonNull final ActionListener<String> mappingTransformListener) {
        if (template == null || template.mappings() == null) {
            mappingTransformListener.onResponse(null);
        } else {
            mappingTransformerRegistry.applyTransformers(template.mappings().string(), null, mappingTransformListener);
        }
    }
}
