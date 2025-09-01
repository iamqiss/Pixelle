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

package org.density.action.admin.indices.get;

import org.density.action.support.ActionFilters;
import org.density.action.support.clustermanager.info.TransportClusterInfoAction;
import org.density.cluster.ClusterState;
import org.density.cluster.metadata.AliasMetadata;
import org.density.cluster.metadata.Context;
import org.density.cluster.metadata.IndexMetadata;
import org.density.cluster.metadata.IndexNameExpressionResolver;
import org.density.cluster.metadata.MappingMetadata;
import org.density.cluster.service.ClusterService;
import org.density.common.inject.Inject;
import org.density.common.settings.IndexScopedSettings;
import org.density.common.settings.Settings;
import org.density.common.settings.SettingsFilter;
import org.density.core.action.ActionListener;
import org.density.core.common.io.stream.StreamInput;
import org.density.indices.IndicesService;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportService;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Get index action.
 *
 * @density.internal
 */
public class TransportGetIndexAction extends TransportClusterInfoAction<GetIndexRequest, GetIndexResponse> {

    private final IndicesService indicesService;
    private final IndexScopedSettings indexScopedSettings;
    private final SettingsFilter settingsFilter;

    @Inject
    public TransportGetIndexAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        SettingsFilter settingsFilter,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        IndicesService indicesService,
        IndexScopedSettings indexScopedSettings
    ) {
        super(
            GetIndexAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetIndexRequest::new,
            indexNameExpressionResolver
        );
        this.indicesService = indicesService;
        this.settingsFilter = settingsFilter;
        this.indexScopedSettings = indexScopedSettings;
    }

    @Override
    protected GetIndexResponse read(StreamInput in) throws IOException {
        return new GetIndexResponse(in);
    }

    @Override
    protected void doClusterManagerOperation(
        final GetIndexRequest request,
        String[] concreteIndices,
        final ClusterState state,
        final ActionListener<GetIndexResponse> listener
    ) {
        Map<String, MappingMetadata> mappingsResult = Map.of();
        Map<String, List<AliasMetadata>> aliasesResult = Map.of();
        Map<String, Settings> settings = Map.of();
        Map<String, Settings> defaultSettings = Map.of();
        Map<String, Context> contexts = Map.of();
        final Map<String, String> dataStreams = new HashMap<>(
            StreamSupport.stream(Spliterators.spliterator(state.metadata().findDataStreams(concreteIndices).entrySet(), 0), false)
                .collect(Collectors.toMap(k -> k.getKey(), v -> v.getValue().getName()))
        );
        GetIndexRequest.Feature[] features = request.features();
        boolean doneAliases = false;
        boolean doneMappings = false;
        boolean doneSettings = false;
        boolean doneContext = false;
        for (GetIndexRequest.Feature feature : features) {
            switch (feature) {
                case MAPPINGS:
                    if (!doneMappings) {
                        try {
                            mappingsResult = state.metadata().findMappings(concreteIndices, indicesService.getFieldFilter());
                            doneMappings = true;
                        } catch (IOException e) {
                            listener.onFailure(e);
                            return;
                        }
                    }
                    break;
                case ALIASES:
                    if (doneAliases == false) {
                        aliasesResult = state.metadata().findAllAliases(concreteIndices);
                        doneAliases = true;
                    }
                    break;
                case SETTINGS:
                    if (!doneSettings) {
                        final Map<String, Settings> settingsMapBuilder = new HashMap<>();
                        final Map<String, Settings> defaultSettingsMapBuilder = new HashMap<>();
                        for (String index : concreteIndices) {
                            Settings indexSettings = state.metadata().index(index).getSettings();
                            if (request.humanReadable()) {
                                indexSettings = IndexMetadata.addHumanReadableSettings(indexSettings);
                            }
                            settingsMapBuilder.put(index, indexSettings);
                            if (request.includeDefaults()) {
                                Settings defaultIndexSettings = settingsFilter.filter(
                                    indexScopedSettings.diff(indexSettings, Settings.EMPTY)
                                );
                                defaultSettingsMapBuilder.put(index, defaultIndexSettings);
                            }
                        }
                        settings = settingsMapBuilder;
                        defaultSettings = defaultSettingsMapBuilder;
                        doneSettings = true;
                    }
                    break;
                case CONTEXT:
                    if (!doneContext) {
                        final Map<String, Context> contextBuilder = new HashMap<>();
                        for (String index : concreteIndices) {
                            Context indexContext = state.metadata().index(index).context();
                            if (indexContext != null) {
                                contextBuilder.put(index, indexContext);
                            }
                        }
                        contexts = contextBuilder;
                        doneContext = true;
                    }
                    break;
                default:
                    throw new IllegalStateException("feature [" + feature + "] is not valid");
            }
        }
        listener.onResponse(
            new GetIndexResponse(concreteIndices, mappingsResult, aliasesResult, settings, defaultSettings, dataStreams, contexts)
        );
    }
}
