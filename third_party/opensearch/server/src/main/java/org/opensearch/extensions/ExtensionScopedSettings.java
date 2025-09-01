/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.extensions;

import org.density.common.settings.AbstractScopedSettings;
import org.density.common.settings.Setting;
import org.density.common.settings.Setting.Property;
import org.density.common.settings.SettingUpgrader;
import org.density.common.settings.Settings;

import java.util.Collections;
import java.util.Set;

/**
 * Encapsulates all valid extension level settings.
 *
 * @density.internal
 */
public final class ExtensionScopedSettings extends AbstractScopedSettings {

    public ExtensionScopedSettings(final Set<Setting<?>> settingsSet) {
        this(settingsSet, Collections.emptySet());
    }

    public ExtensionScopedSettings(final Set<Setting<?>> settingsSet, final Set<SettingUpgrader<?>> settingUpgraders) {
        super(Settings.EMPTY, settingsSet, settingUpgraders, Property.ExtensionScope);
    }
}
