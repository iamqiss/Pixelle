/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.cache;

import org.density.cache.store.disk.EhcacheDiskCache;
import org.density.common.cache.ICache;
import org.density.test.DensityTestCase;

import java.util.Map;

public class EhcachePluginTests extends DensityTestCase {

    private EhcacheCachePlugin ehcacheCachePlugin = new EhcacheCachePlugin();

    public void testGetCacheStoreTypeMap() {
        Map<String, ICache.Factory> factoryMap = ehcacheCachePlugin.getCacheFactoryMap();
        assertNotNull(factoryMap);
        assertNotNull(factoryMap.get(EhcacheDiskCache.EhcacheDiskCacheFactory.EHCACHE_DISK_CACHE_NAME));
    }
}
