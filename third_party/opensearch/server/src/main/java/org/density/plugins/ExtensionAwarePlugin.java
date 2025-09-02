/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.plugins;

import org.density.common.settings.Setting;

import java.util.Collections;
import java.util.List;

/**
 * Plugin that provides extra settings for extensions
 *
 * @density.experimental
 */
public interface ExtensionAwarePlugin {

    /**
     * Returns a list of additional {@link Setting} definitions that this plugin adds for extensions
     */
    default List<Setting<?>> getExtensionSettings() {
        return Collections.emptyList();
    }
}
