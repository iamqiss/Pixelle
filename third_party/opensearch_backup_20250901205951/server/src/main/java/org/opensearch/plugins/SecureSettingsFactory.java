/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.plugins;

import org.density.common.annotation.ExperimentalApi;
import org.density.common.settings.Settings;

import java.util.Optional;

/**
 * A factory for creating the instance of the {@link SecureTransportSettingsProvider}, taking into account current settings.
 *
 * @density.experimental
 */
@ExperimentalApi
public interface SecureSettingsFactory {
    /**
     * Creates (or provides pre-created) instance of the {@link SecureTransportSettingsProvider}
     * @param settings settings
     * @return optionally, the instance of the {@link SecureTransportSettingsProvider}
     */
    Optional<SecureTransportSettingsProvider> getSecureTransportSettingsProvider(Settings settings);

    /**
     * Creates (or provides pre-created) instance of the {@link SecureHttpTransportSettingsProvider}
     * @param settings settings
     * @return optionally, the instance of the {@link SecureHttpTransportSettingsProvider}
     */
    Optional<SecureHttpTransportSettingsProvider> getSecureHttpTransportSettingsProvider(Settings settings);

    /**
     * Creates (or provides pre-created) instance of the {@link SecureAuxTransportSettingsProvider}
     * @param settings settings
     * @return optionally, the instance of the {@link SecureAuxTransportSettingsProvider}
     */
    Optional<SecureAuxTransportSettingsProvider> getSecureAuxTransportSettingsProvider(Settings settings);
}
