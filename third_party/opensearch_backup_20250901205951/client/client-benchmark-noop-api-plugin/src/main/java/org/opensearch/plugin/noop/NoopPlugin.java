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

package org.density.plugin.noop;

import org.density.action.ActionRequest;
import org.density.cluster.metadata.IndexNameExpressionResolver;
import org.density.cluster.node.DiscoveryNodes;
import org.density.common.settings.ClusterSettings;
import org.density.common.settings.IndexScopedSettings;
import org.density.common.settings.Settings;
import org.density.common.settings.SettingsFilter;
import org.density.core.action.ActionResponse;
import org.density.plugin.noop.action.bulk.NoopBulkAction;
import org.density.plugin.noop.action.bulk.RestNoopBulkAction;
import org.density.plugin.noop.action.bulk.TransportNoopBulkAction;
import org.density.plugin.noop.action.search.NoopSearchAction;
import org.density.plugin.noop.action.search.RestNoopSearchAction;
import org.density.plugin.noop.action.search.TransportNoopSearchAction;
import org.density.plugins.ActionPlugin;
import org.density.plugins.Plugin;
import org.density.rest.RestController;
import org.density.rest.RestHandler;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

public class NoopPlugin extends Plugin implements ActionPlugin {
    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays.asList(
            new ActionHandler<>(NoopBulkAction.INSTANCE, TransportNoopBulkAction.class),
            new ActionHandler<>(NoopSearchAction.INSTANCE, TransportNoopSearchAction.class)
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
        return Arrays.asList(new RestNoopBulkAction(), new RestNoopSearchAction());
    }
}
