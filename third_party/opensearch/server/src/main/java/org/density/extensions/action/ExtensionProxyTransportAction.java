/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.extensions.action;

import org.density.action.support.ActionFilters;
import org.density.action.support.HandledTransportAction;
import org.density.cluster.service.ClusterService;
import org.density.common.inject.Inject;
import org.density.common.settings.Settings;
import org.density.core.action.ActionListener;
import org.density.extensions.ExtensionsManager;
import org.density.tasks.Task;
import org.density.transport.TransportService;

/**
 * A proxy transport action used to proxy a transport request from Density or a plugin to execute on an extension
 *
 * @density.internal
 */
public class ExtensionProxyTransportAction extends HandledTransportAction<ExtensionActionRequest, ExtensionActionResponse> {

    private final ExtensionsManager extensionsManager;

    @Inject
    public ExtensionProxyTransportAction(
        Settings settings,
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        ExtensionsManager extensionsManager
    ) {
        super(ExtensionProxyAction.NAME, transportService, actionFilters, ExtensionActionRequest::new);
        this.extensionsManager = extensionsManager;
    }

    @Override
    protected void doExecute(Task task, ExtensionActionRequest request, ActionListener<ExtensionActionResponse> listener) {
        try {
            listener.onResponse(extensionsManager.handleTransportRequest(request));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
