/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.extensions;

import org.density.action.ActionModule;
import org.density.cluster.service.ClusterService;
import org.density.common.settings.Settings;
import org.density.common.settings.SettingsModule;
import org.density.extensions.action.ExtensionActionRequest;
import org.density.extensions.action.ExtensionActionResponse;
import org.density.extensions.action.RemoteExtensionActionResponse;
import org.density.identity.IdentityService;
import org.density.transport.TransportService;
import org.density.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.Optional;
import java.util.Set;

/**
 * Noop class for ExtensionsManager
 *
 * @density.internal
 */
public class NoopExtensionsManager extends ExtensionsManager {

    public NoopExtensionsManager(IdentityService identityService) throws IOException {
        super(Set.of(), identityService);
    }

    @Override
    public void initializeServicesAndRestHandler(
        ActionModule actionModule,
        SettingsModule settingsModule,
        TransportService transportService,
        ClusterService clusterService,
        Settings initialEnvironmentSettings,
        NodeClient client,
        IdentityService identityService
    ) {
        // no-op
    }

    @Override
    public RemoteExtensionActionResponse handleRemoteTransportRequest(ExtensionActionRequest request) throws Exception {
        // no-op empty response
        return new RemoteExtensionActionResponse(true, new byte[0]);
    }

    @Override
    public ExtensionActionResponse handleTransportRequest(ExtensionActionRequest request) throws Exception {
        // no-op empty response
        return new ExtensionActionResponse(new byte[0]);
    }

    @Override
    public void initialize() {
        // no-op
    }

    @Override
    public Optional<DiscoveryExtensionNode> lookupInitializedExtensionById(final String extensionId) {
        // no-op not found
        return Optional.empty();
    }
}
