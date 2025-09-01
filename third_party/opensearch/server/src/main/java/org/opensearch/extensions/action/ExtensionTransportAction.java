/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.extensions.action;

import org.density.action.support.ActionFilters;
import org.density.action.support.TransportAction;
import org.density.core.action.ActionListener;
import org.density.extensions.ExtensionsManager;
import org.density.tasks.Task;
import org.density.tasks.TaskManager;

/**
 * A proxy transport action used to proxy a transport request from an extension to execute on another extension
 *
 * @density.internal
 */
public class ExtensionTransportAction extends TransportAction<ExtensionActionRequest, RemoteExtensionActionResponse> {

    private final ExtensionsManager extensionsManager;

    public ExtensionTransportAction(
        String actionName,
        ActionFilters actionFilters,
        TaskManager taskManager,
        ExtensionsManager extensionsManager
    ) {
        super(actionName, actionFilters, taskManager);
        this.extensionsManager = extensionsManager;
    }

    @Override
    protected void doExecute(Task task, ExtensionActionRequest request, ActionListener<RemoteExtensionActionResponse> listener) {
        try {
            listener.onResponse(extensionsManager.handleRemoteTransportRequest(request));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
