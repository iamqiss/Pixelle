/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.script.mustache;

import org.density.action.support.ActionFilters;
import org.density.common.inject.Inject;
import org.density.core.xcontent.NamedXContentRegistry;
import org.density.script.ScriptService;
import org.density.transport.TransportService;
import org.density.transport.client.node.NodeClient;

public class TransportRenderSearchTemplateAction extends TransportSearchTemplateAction {

    @Inject
    public TransportRenderSearchTemplateAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        NodeClient client
    ) {
        super(RenderSearchTemplateAction.NAME, transportService, actionFilters, scriptService, xContentRegistry, client);
    }
}
