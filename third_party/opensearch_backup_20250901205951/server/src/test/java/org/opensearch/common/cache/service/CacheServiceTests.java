/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.common.cache.service;

import org.density.common.cache.CacheType;
import org.density.common.cache.ICache;
import org.density.common.cache.RemovalListener;
import org.density.common.cache.module.CacheModule;
import org.density.common.cache.settings.CacheSettings;
import org.density.common.cache.store.DensityOnHeapCache;
import org.density.common.cache.store.config.CacheConfig;
import org.density.common.settings.Setting;
import org.density.common.settings.Settings;
import org.density.plugins.CachePlugin;
import org.density.test.DensityTestCase;

import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CacheServiceTests extends DensityTestCase {
    public void testWithCreateCacheForIndicesRequestCacheType() {
        CachePlugin mockPlugin1 = mock(CachePlugin.class);
        ICache.Factory factory1 = mock(ICache.Factory.class);
        ICache.Factory onHeapCacheFactory = mock(DensityOnHeapCache.DensityOnHeapCacheFactory.class);
        Map<String, ICache.Factory> factoryMap = Map.of(
            "cache1",
            factory1,
            DensityOnHeapCache.DensityOnHeapCacheFactory.NAME,
            onHeapCacheFactory
        );
        when(mockPlugin1.getCacheFactoryMap()).thenReturn(factoryMap);

        Setting<String> indicesRequestCacheSetting = CacheSettings.getConcreteStoreNameSettingForCacheType(CacheType.INDICES_REQUEST_CACHE);
        CacheService cacheService = new CacheService(
            factoryMap,
            Settings.builder().put(indicesRequestCacheSetting.getKey(), "cache1").build()
        );
        CacheConfig<String, String> config = mock(CacheConfig.class);
        ICache<String, String> mockOnHeapCache = mock(DensityOnHeapCache.class);
        when(factory1.create(eq(config), eq(CacheType.INDICES_REQUEST_CACHE), any(Map.class))).thenReturn(mockOnHeapCache);
        ICache<String, String> otherMockOnHeapCache = mock(DensityOnHeapCache.class);
        when(onHeapCacheFactory.create(eq(config), eq(CacheType.INDICES_REQUEST_CACHE), any(Map.class))).thenReturn(otherMockOnHeapCache);

        ICache<String, String> ircCache = cacheService.createCache(config, CacheType.INDICES_REQUEST_CACHE);
        assertEquals(mockOnHeapCache, ircCache);
    }

    public void testWithCreateCacheForIndicesRequestCacheTypeWithStoreNameNull() {
        CachePlugin mockPlugin1 = mock(CachePlugin.class);
        ICache.Factory factory1 = mock(ICache.Factory.class);
        ICache.Factory onHeapCacheFactory = mock(DensityOnHeapCache.DensityOnHeapCacheFactory.class);
        Map<String, ICache.Factory> factoryMap = Map.of(
            "cache1",
            factory1,
            DensityOnHeapCache.DensityOnHeapCacheFactory.NAME,
            onHeapCacheFactory
        );
        when(mockPlugin1.getCacheFactoryMap()).thenReturn(factoryMap);

        CacheService cacheService = new CacheService(factoryMap, Settings.builder().build());
        CacheConfig<String, String> config = mock(CacheConfig.class);
        ICache<String, String> mockOnHeapCache = mock(DensityOnHeapCache.class);
        when(onHeapCacheFactory.create(eq(config), eq(CacheType.INDICES_REQUEST_CACHE), any(Map.class))).thenReturn(mockOnHeapCache);

        ICache<String, String> ircCache = cacheService.createCache(config, CacheType.INDICES_REQUEST_CACHE);
        assertEquals(mockOnHeapCache, ircCache);
    }

    public void testWithCreateCacheWithNoStoreNamePresentForCacheType() {
        ICache.Factory factory1 = mock(ICache.Factory.class);
        Map<String, ICache.Factory> factoryMap = Map.of("cache1", factory1);
        CacheService cacheService = new CacheService(factoryMap, Settings.builder().build());

        CacheConfig<String, String> config = mock(CacheConfig.class);
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> cacheService.createCache(config, CacheType.INDICES_REQUEST_CACHE)
        );
        assertEquals("No store name: [density_onheap] is registered for cache type: INDICES_REQUEST_CACHE", ex.getMessage());
    }

    public void testWithCreateCacheWithDefaultStoreNameForIRC() {
        CachePlugin mockPlugin1 = mock(CachePlugin.class);
        ICache.Factory factory1 = mock(ICache.Factory.class);
        Map<String, ICache.Factory> factoryMap = Map.of("cache1", factory1);
        when(mockPlugin1.getCacheFactoryMap()).thenReturn(factoryMap);

        CacheModule cacheModule = new CacheModule(List.of(mockPlugin1), Settings.EMPTY);
        CacheConfig<String, String> config = mock(CacheConfig.class);
        when(config.getSettings()).thenReturn(Settings.EMPTY);
        when(config.getWeigher()).thenReturn((k, v) -> 100);
        when(config.getRemovalListener()).thenReturn(mock(RemovalListener.class));

        CacheService cacheService = cacheModule.getCacheService();
        ICache<String, String> iCache = cacheService.createCache(config, CacheType.INDICES_REQUEST_CACHE);
        assertTrue(iCache instanceof DensityOnHeapCache);
    }

    public void testWithCreateCacheWithInvalidStoreNameAssociatedForCacheType() {
        ICache.Factory factory1 = mock(ICache.Factory.class);
        Setting<String> indicesRequestCacheSetting = CacheSettings.getConcreteStoreNameSettingForCacheType(CacheType.INDICES_REQUEST_CACHE);
        Map<String, ICache.Factory> factoryMap = Map.of("cache1", factory1);
        CacheService cacheService = new CacheService(
            factoryMap,
            Settings.builder().put(indicesRequestCacheSetting.getKey(), "cache").build()
        );

        CacheConfig<String, String> config = mock(CacheConfig.class);
        ICache<String, String> onHeapCache = mock(DensityOnHeapCache.class);
        when(factory1.create(config, CacheType.INDICES_REQUEST_CACHE, factoryMap)).thenReturn(onHeapCache);

        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> cacheService.createCache(config, CacheType.INDICES_REQUEST_CACHE)
        );
        assertEquals("No store name: [cache] is registered for cache type: INDICES_REQUEST_CACHE", ex.getMessage());
    }
}
