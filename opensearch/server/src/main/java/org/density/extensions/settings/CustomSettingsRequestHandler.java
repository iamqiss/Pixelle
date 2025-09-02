/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.extensions.settings;

import org.density.common.settings.Setting;
import org.density.common.settings.SettingsModule;
import org.density.core.transport.TransportResponse;
import org.density.extensions.AcknowledgedResponse;

import java.util.ArrayList;
import java.util.List;

/**
 * Handles requests to register a list of custom extension settings.
 *
 * @density.internal
 */
public class CustomSettingsRequestHandler {

    private final SettingsModule settingsModule;

    /**
     * Instantiates a new Settings Request Handler using the Node's SettingsModule.
     *
     * @param settingsModule  The Node's {@link SettingsModule}.
     */
    public CustomSettingsRequestHandler(SettingsModule settingsModule) {
        this.settingsModule = settingsModule;
    }

    /**
     * Handles a {@link RegisterCustomSettingsRequest}.
     *
     * @param customSettingsRequest  The request to handle.
     * @return A {@link AcknowledgedResponse} indicating success.
     * @throws Exception if the request is not handled properly.
     */
    public TransportResponse handleRegisterCustomSettingsRequest(RegisterCustomSettingsRequest customSettingsRequest) throws Exception {
        // TODO: How do we prevent key collisions in settings registration?
        // we have settingsRequest.getUniqueId() available or could enforce reverse DNS naming
        // See https://github.com/density-project/density-sdk-java/issues/142
        List<String> registeredCustomSettings = new ArrayList<>();
        for (Setting<?> setting : customSettingsRequest.getSettings()) {
            settingsModule.registerDynamicSetting(setting);
            registeredCustomSettings.add(setting.getKey());
        }
        return new AcknowledgedResponse(true);
    }
}
