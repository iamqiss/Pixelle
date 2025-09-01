/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.extensions;

import org.density.common.settings.Settings;
import org.density.common.util.FeatureFlags;
import org.density.test.DensityIntegTestCase;

@DensityIntegTestCase.ClusterScope(scope = DensityIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ExtensionsManagerIT extends DensityIntegTestCase {

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder().put(super.featureFlagSettings()).put(FeatureFlags.EXTENSIONS, "true").build();
    }

    public void testExtensionsManagerCreation() {
        String nodeName = internalCluster().startNode();

        ensureGreen();

        ExtensionsManager extManager = internalCluster().getInstance(ExtensionsManager.class, nodeName);

        assertEquals(ExtensionsManager.class.getName(), extManager.getClass().getName());
    }
}
