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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.density.action.support.ActionFilters;
import org.density.action.support.clustermanager.AcknowledgedResponse;
import org.density.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.density.cluster.ClusterState;
import org.density.cluster.block.ClusterBlockException;
import org.density.cluster.block.ClusterBlockLevel;
import org.density.cluster.metadata.IndexMetadata;
import org.density.cluster.metadata.IndexNameExpressionResolver;
import org.density.cluster.metadata.MetadataIndexTemplateService;
import org.density.cluster.service.ClusterService;
import org.density.common.inject.Inject;
import org.density.common.settings.IndexScopedSettings;
import org.density.common.settings.Settings;
import org.density.core.action.ActionListener;
import org.density.core.common.io.stream.StreamInput;
import org.density.index.mapper.MappingTransformerRegistry;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportService;

import java.io.IOException;

/**
 * Put index template action.
 *
 * @density.internal
 */
public class TransportPutIndexTemplateAction extends TransportClusterManagerNodeAction<PutIndexTemplateRequest, AcknowledgedResponse> {

    private static final Logger logger = LogManager.getLogger(TransportPutIndexTemplateAction.class);

    private final MetadataIndexTemplateService indexTemplateService;
    private final IndexScopedSettings indexScopedSettings;
    private final MappingTransformerRegistry mappingTransformerRegistry;

    @Inject
    public TransportPutIndexTemplateAction(
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
            PutIndexTemplateAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PutIndexTemplateRequest::new,
            indexNameExpressionResolver
        );
        this.indexTemplateService = indexTemplateService;
        this.indexScopedSettings = indexScopedSettings;
        this.mappingTransformerRegistry = mappingTransformerRegistry;
    }

    @Override
    protected String executor() {
        // we go async right away...
        return ThreadPool.Names.SAME;
    }

    @Override
    protected AcknowledgedResponse read(StreamInput in) throws IOException {
        return new AcknowledgedResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(PutIndexTemplateRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void clusterManagerOperation(
        final PutIndexTemplateRequest request,
        final ClusterState state,
        final ActionListener<AcknowledgedResponse> listener
    ) {
        String cause = request.cause();
        if (cause.length() == 0) {
            cause = "api";
        }
        final Settings.Builder templateSettingsBuilder = Settings.builder();
        templateSettingsBuilder.put(request.settings()).normalizePrefix(IndexMetadata.INDEX_SETTING_PREFIX);
        indexScopedSettings.validate(templateSettingsBuilder.build(), true); // templates must be consistent with regards to dependencies
        final String finalCause = cause;
        final ActionListener<String> mappingTransformListener = ActionListener.wrap(transformedMappings -> {
            indexTemplateService.putTemplate(
                new MetadataIndexTemplateService.PutRequest(finalCause, request.name()).patterns(request.patterns())
                    .order(request.order())
                    .settings(templateSettingsBuilder.build())
                    .mappings(transformedMappings)
                    .aliases(request.aliases())
                    .create(request.create())
                    .clusterManagerTimeout(request.clusterManagerNodeTimeout())
                    .version(request.version()),

                new MetadataIndexTemplateService.PutListener() {
                    @Override
                    public void onResponse(MetadataIndexTemplateService.PutResponse response) {
                        listener.onResponse(new AcknowledgedResponse(response.acknowledged()));
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.debug(() -> new ParameterizedMessage("failed to put template [{}]", request.name()), e);
                        listener.onFailure(e);
                    }
                }
            );
        }, listener::onFailure);

        mappingTransformerRegistry.applyTransformers(request.mappings(), null, mappingTransformListener);
    }
}
