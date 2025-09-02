/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.common.cache.module;

import org.density.common.annotation.ExperimentalApi;
import org.density.common.cache.ICache;
import org.density.common.cache.service.CacheService;
import org.density.common.cache.store.DensityOnHeapCache;
import org.density.common.settings.Settings;
import org.density.plugins.CachePlugin;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Holds all the cache factories and provides a way to fetch them when needed.
 */
@ExperimentalApi
public class CacheModule {

    private final Map<String, ICache.Factory> cacheStoreTypeFactories;

    private final CacheService cacheService;
    private final Settings settings;

    public CacheModule(List<CachePlugin> cachePlugins, Settings settings) {
        this.cacheStoreTypeFactories = getCacheStoreTypeFactories(cachePlugins);
        this.settings = settings;
        this.cacheService = new CacheService(cacheStoreTypeFactories, settings);
    }

    private static Map<String, ICache.Factory> getCacheStoreTypeFactories(List<CachePlugin> cachePlugins) {
        Map<String, ICache.Factory> cacheStoreTypeFactories = new HashMap<>();
        // Add the core DensityOnHeapCache as well.
        cacheStoreTypeFactories.put(
            DensityOnHeapCache.DensityOnHeapCacheFactory.NAME,
            new DensityOnHeapCache.DensityOnHeapCacheFactory()
        );
        for (CachePlugin cachePlugin : cachePlugins) {
            Map<String, ICache.Factory> factoryMap = cachePlugin.getCacheFactoryMap();
            for (Map.Entry<String, ICache.Factory> entry : factoryMap.entrySet()) {
                if (cacheStoreTypeFactories.put(entry.getKey(), entry.getValue()) != null) {
                    throw new IllegalArgumentException("Cache name: " + entry.getKey() + " is " + "already registered");
                }
            }
        }
        return Collections.unmodifiableMap(cacheStoreTypeFactories);
    }

    public CacheService getCacheService() {
        return this.cacheService;
    }

    // Package private for testing.
    Map<String, ICache.Factory> getCacheStoreTypeFactories() {
        return cacheStoreTypeFactories;
    }
}
