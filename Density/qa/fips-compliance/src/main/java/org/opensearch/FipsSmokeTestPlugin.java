/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density;

import org.density.cluster.metadata.IndexNameExpressionResolver;
import org.density.cluster.node.DiscoveryNodes;
import org.density.common.settings.ClusterSettings;
import org.density.common.settings.IndexScopedSettings;
import org.density.common.settings.Settings;
import org.density.common.settings.SettingsFilter;
import org.density.plugins.ActionPlugin;
import org.density.plugins.Plugin;
import org.density.rest.RestController;
import org.density.rest.RestHandler;

import java.util.List;
import java.util.function.Supplier;

public class FipsSmokeTestPlugin extends Plugin implements ActionPlugin {

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
        return List.of(new RandomnessProviderNameAction());
    }
}
