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

package org.density.dashboards;

import org.density.cluster.metadata.IndexNameExpressionResolver;
import org.density.cluster.node.DiscoveryNodes;
import org.density.common.settings.ClusterSettings;
import org.density.common.settings.IndexScopedSettings;
import org.density.common.settings.Setting;
import org.density.common.settings.Setting.Property;
import org.density.common.settings.Settings;
import org.density.common.settings.SettingsFilter;
import org.density.index.reindex.RestDeleteByQueryAction;
import org.density.indices.SystemIndexDescriptor;
import org.density.plugins.Plugin;
import org.density.plugins.SystemIndexPlugin;
import org.density.rest.BaseRestHandler;
import org.density.rest.RestController;
import org.density.rest.RestHandler;
import org.density.rest.action.admin.indices.RestCreateIndexAction;
import org.density.rest.action.admin.indices.RestGetAliasesAction;
import org.density.rest.action.admin.indices.RestGetIndicesAction;
import org.density.rest.action.admin.indices.RestIndexPutAliasAction;
import org.density.rest.action.admin.indices.RestRefreshAction;
import org.density.rest.action.admin.indices.RestUpdateSettingsAction;
import org.density.rest.action.document.RestBulkAction;
import org.density.rest.action.document.RestBulkStreamingAction;
import org.density.rest.action.document.RestDeleteAction;
import org.density.rest.action.document.RestGetAction;
import org.density.rest.action.document.RestIndexAction;
import org.density.rest.action.document.RestIndexAction.AutoIdHandler;
import org.density.rest.action.document.RestIndexAction.CreateHandler;
import org.density.rest.action.document.RestMultiGetAction;
import org.density.rest.action.document.RestUpdateAction;
import org.density.rest.action.search.RestClearScrollAction;
import org.density.rest.action.search.RestSearchAction;
import org.density.rest.action.search.RestSearchScrollAction;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableList;

public class DensityDashboardsModulePlugin extends Plugin implements SystemIndexPlugin {

    public static final Setting<List<String>> DENSITY_DASHBOARDS_INDEX_NAMES_SETTING = Setting.listSetting(
        "density_dashboards.system_indices",
        unmodifiableList(
            Arrays.asList(
                ".density_dashboards",
                ".density_dashboards_*",
                ".reporting-*",
                ".apm-agent-configuration",
                ".apm-custom-link"
            )
        ),
        Function.identity(),
        Property.NodeScope
    );

    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
        return unmodifiableList(
            DENSITY_DASHBOARDS_INDEX_NAMES_SETTING.get(settings)
                .stream()
                .map(pattern -> new SystemIndexDescriptor(pattern, "System index used by Density Dashboards"))
                .collect(Collectors.toList())
        );
    }

    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        // TODO need to figure out what subset of system indices Density Dashboards should have access to via these APIs
        return unmodifiableList(
            Arrays.asList(
                // Based on https://github.com/elastic/kibana/issues/49764
                // apis needed to perform migrations... ideally these will go away
                new DensityDashboardsWrappedRestHandler(new RestCreateIndexAction()),
                new DensityDashboardsWrappedRestHandler(new RestGetAliasesAction()),
                new DensityDashboardsWrappedRestHandler(new RestIndexPutAliasAction()),
                new DensityDashboardsWrappedRestHandler(new RestRefreshAction()),

                // apis needed to access saved objects
                new DensityDashboardsWrappedRestHandler(new RestGetAction()),
                new DensityDashboardsWrappedRestHandler(new RestMultiGetAction(settings)),
                new DensityDashboardsWrappedRestHandler(new RestSearchAction()),
                new DensityDashboardsWrappedRestHandler(new RestBulkAction(settings)),
                new DensityDashboardsWrappedRestHandler(new RestBulkStreamingAction(settings)),
                new DensityDashboardsWrappedRestHandler(new RestDeleteAction()),
                new DensityDashboardsWrappedRestHandler(new RestDeleteByQueryAction()),

                // api used for testing
                new DensityDashboardsWrappedRestHandler(new RestUpdateSettingsAction()),

                // apis used specifically by reporting
                new DensityDashboardsWrappedRestHandler(new RestGetIndicesAction()),
                new DensityDashboardsWrappedRestHandler(new RestIndexAction()),
                new DensityDashboardsWrappedRestHandler(new CreateHandler()),
                new DensityDashboardsWrappedRestHandler(new AutoIdHandler(nodesInCluster)),
                new DensityDashboardsWrappedRestHandler(new RestUpdateAction()),
                new DensityDashboardsWrappedRestHandler(new RestSearchScrollAction()),
                new DensityDashboardsWrappedRestHandler(new RestClearScrollAction())
            )
        );

    }

    @Override
    public List<Setting<?>> getSettings() {
        return Collections.singletonList(DENSITY_DASHBOARDS_INDEX_NAMES_SETTING);
    }

    static class DensityDashboardsWrappedRestHandler extends BaseRestHandler.Wrapper {

        DensityDashboardsWrappedRestHandler(BaseRestHandler delegate) {
            super(delegate);
        }

        @Override
        public String getName() {
            return "density_dashboards_" + super.getName();
        }

        @Override
        public boolean allowSystemIndexAccessByDefault() {
            return true;
        }

        @Override
        public List<Route> routes() {
            return unmodifiableList(
                super.routes().stream()
                    .map(route -> new Route(route.getMethod(), "/_density_dashboards" + route.getPath()))
                    .collect(Collectors.toList())
            );
        }
    }
}
