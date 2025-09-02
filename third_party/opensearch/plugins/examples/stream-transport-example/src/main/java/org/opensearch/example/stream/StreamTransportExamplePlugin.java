/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.example.stream;

import org.density.action.ActionRequest;
import org.density.core.action.ActionResponse;
import org.density.plugins.ActionPlugin;
import org.density.plugins.Plugin;

import java.util.Collections;
import java.util.List;

/**
 * Example plugin demonstrating streaming transport actions
 */
public class StreamTransportExamplePlugin extends Plugin implements ActionPlugin {

    /**
     * Constructor
     */
    public StreamTransportExamplePlugin() {}

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Collections.singletonList(new ActionHandler<>(StreamDataAction.INSTANCE, TransportStreamDataAction.class));
    }
}
