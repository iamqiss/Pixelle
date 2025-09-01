/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.common.cache.module;

import org.density.common.cache.ICache;
import org.density.common.settings.Settings;
import org.density.plugins.CachePlugin;
import org.density.test.DensityTestCase;

import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CacheModuleTests extends DensityTestCase {

    public void testWithMultiplePlugins() {
        CachePlugin mockPlugin1 = mock(CachePlugin.class);
        ICache.Factory factory1 = mock(ICache.Factory.class);
        CachePlugin mockPlugin2 = mock(CachePlugin.class);
        ICache.Factory factory2 = mock(ICache.Factory.class);
        when(mockPlugin1.getCacheFactoryMap()).thenReturn(Map.of("cache1", factory1));
        when(mockPlugin2.getCacheFactoryMap()).thenReturn(Map.of("cache2", factory2));

        CacheModule cacheModule = new CacheModule(List.of(mockPlugin1, mockPlugin2), Settings.EMPTY);

        Map<String, ICache.Factory> factoryMap = cacheModule.getCacheStoreTypeFactories();
        assertEquals(factoryMap.get("cache1"), factory1);
        assertEquals(factoryMap.get("cache2"), factory2);
    }

    public void testWithSameCacheStoreTypeAndName() {
        CachePlugin mockPlugin1 = mock(CachePlugin.class);
        ICache.Factory factory1 = mock(ICache.Factory.class);
        CachePlugin mockPlugin2 = mock(CachePlugin.class);
        ICache.Factory factory2 = mock(ICache.Factory.class);
        when(factory1.getCacheName()).thenReturn("cache");
        when(factory2.getCacheName()).thenReturn("cache");
        when(mockPlugin1.getCacheFactoryMap()).thenReturn(Map.of("cache", factory1));
        when(mockPlugin2.getCacheFactoryMap()).thenReturn(Map.of("cache", factory2));

        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> new CacheModule(List.of(mockPlugin1, mockPlugin2), Settings.EMPTY)
        );
        assertEquals("Cache name: cache is already registered", ex.getMessage());
    }
}
