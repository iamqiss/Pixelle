/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.indices.recovery;

import org.density.common.settings.ClusterSettings;
import org.density.common.settings.Settings;

/**
 * Utility to provide a {@link RecoverySettings} instance containing all defaults
 */
public final class DefaultRecoverySettings {
    private DefaultRecoverySettings() {}

    public static final RecoverySettings INSTANCE = new RecoverySettings(
        Settings.EMPTY,
        new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
    );
}
