/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.cache.common.tier;

import org.density.common.cache.ICache;
import org.density.common.settings.Settings;
import org.density.test.DensityTestCase;

import java.util.Map;

public class TieredSpilloverCachePluginTests extends DensityTestCase {

    public void testGetCacheFactoryMap() {
        TieredSpilloverCachePlugin tieredSpilloverCachePlugin = new TieredSpilloverCachePlugin(Settings.EMPTY);
        Map<String, ICache.Factory> map = tieredSpilloverCachePlugin.getCacheFactoryMap();
        assertNotNull(map.get(TieredSpilloverCache.TieredSpilloverCacheFactory.TIERED_SPILLOVER_CACHE_NAME));
        assertEquals(TieredSpilloverCachePlugin.TIERED_CACHE_SPILLOVER_PLUGIN_NAME, tieredSpilloverCachePlugin.getName());
    }

    public void testGetSettings() {
        TieredSpilloverCachePlugin tieredSpilloverCachePlugin = new TieredSpilloverCachePlugin(Settings.builder().build());
        assertFalse(tieredSpilloverCachePlugin.getSettings().isEmpty());
    }
}
