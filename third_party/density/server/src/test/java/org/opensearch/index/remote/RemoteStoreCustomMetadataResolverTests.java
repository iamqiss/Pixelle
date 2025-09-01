/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.index.remote;

import org.density.Version;
import org.density.common.blobstore.BlobStore;
import org.density.common.settings.ClusterSettings;
import org.density.common.settings.Settings;
import org.density.index.remote.RemoteStoreEnums.PathHashAlgorithm;
import org.density.index.remote.RemoteStoreEnums.PathType;
import org.density.indices.RemoteStoreSettings;
import org.density.repositories.RepositoriesService;
import org.density.repositories.blobstore.BlobStoreRepository;
import org.density.test.DensityTestCase;

import static org.density.indices.RemoteStoreSettings.CLUSTER_REMOTE_STORE_PATH_HASH_ALGORITHM_SETTING;
import static org.density.indices.RemoteStoreSettings.CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING;
import static org.density.indices.RemoteStoreSettings.CLUSTER_REMOTE_STORE_TRANSLOG_METADATA;
import static org.density.node.remotestore.RemoteStoreNodeAttribute.getRemoteStoreTranslogRepo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RemoteStoreCustomMetadataResolverTests extends DensityTestCase {

    RepositoriesService repositoriesService = mock(RepositoriesService.class);

    public void testGetPathStrategyMinVersionOlder() {
        Settings settings = Settings.builder().put(CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING.getKey(), randomFrom(PathType.values())).build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        RemoteStoreSettings remoteStoreSettings = new RemoteStoreSettings(settings, clusterSettings);
        RemoteStoreCustomMetadataResolver resolver = new RemoteStoreCustomMetadataResolver(
            remoteStoreSettings,
            () -> Version.V_2_13_0,
            () -> repositoriesService,
            settings
        );
        assertEquals(PathType.FIXED, resolver.getPathStrategy().getType());
        assertNull(resolver.getPathStrategy().getHashAlgorithm());
    }

    public void testGetPathStrategyMinVersionNewer() {
        PathType pathType = randomFrom(PathType.values());
        Settings settings = Settings.builder().put(CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING.getKey(), pathType).build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        RemoteStoreSettings remoteStoreSettings = new RemoteStoreSettings(settings, clusterSettings);
        RemoteStoreCustomMetadataResolver resolver = new RemoteStoreCustomMetadataResolver(
            remoteStoreSettings,
            () -> Version.V_2_14_0,
            () -> repositoriesService,
            settings
        );
        assertEquals(pathType, resolver.getPathStrategy().getType());
        if (pathType.requiresHashAlgorithm()) {
            assertNotNull(resolver.getPathStrategy().getHashAlgorithm());
        } else {
            assertNull(resolver.getPathStrategy().getHashAlgorithm());
        }
    }

    public void testGetPathStrategyStrategy() {
        // FIXED type
        Settings settings = Settings.builder().put(CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING.getKey(), PathType.FIXED).build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        RemoteStoreSettings remoteStoreSettings = new RemoteStoreSettings(settings, clusterSettings);
        RemoteStoreCustomMetadataResolver resolver = new RemoteStoreCustomMetadataResolver(
            remoteStoreSettings,
            () -> Version.V_2_14_0,
            () -> repositoriesService,
            settings
        );
        assertEquals(PathType.FIXED, resolver.getPathStrategy().getType());

        // FIXED type with hash algorithm
        settings = Settings.builder()
            .put(CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING.getKey(), PathType.FIXED)
            .put(CLUSTER_REMOTE_STORE_PATH_HASH_ALGORITHM_SETTING.getKey(), randomFrom(PathHashAlgorithm.values()))
            .build();
        clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        remoteStoreSettings = new RemoteStoreSettings(settings, clusterSettings);
        resolver = new RemoteStoreCustomMetadataResolver(remoteStoreSettings, () -> Version.V_2_14_0, () -> repositoriesService, settings);
        assertEquals(PathType.FIXED, resolver.getPathStrategy().getType());

        // HASHED_PREFIX type with FNV_1A_COMPOSITE
        settings = Settings.builder().put(CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING.getKey(), PathType.HASHED_PREFIX).build();
        clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        remoteStoreSettings = new RemoteStoreSettings(settings, clusterSettings);
        resolver = new RemoteStoreCustomMetadataResolver(remoteStoreSettings, () -> Version.V_2_14_0, () -> repositoriesService, settings);
        assertEquals(PathType.HASHED_PREFIX, resolver.getPathStrategy().getType());
        assertEquals(PathHashAlgorithm.FNV_1A_COMPOSITE_1, resolver.getPathStrategy().getHashAlgorithm());

        // HASHED_PREFIX type with FNV_1A_COMPOSITE
        settings = Settings.builder().put(CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING.getKey(), PathType.HASHED_PREFIX).build();
        clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        remoteStoreSettings = new RemoteStoreSettings(settings, clusterSettings);
        resolver = new RemoteStoreCustomMetadataResolver(remoteStoreSettings, () -> Version.V_2_14_0, () -> repositoriesService, settings);
        assertEquals(PathType.HASHED_PREFIX, resolver.getPathStrategy().getType());
        assertEquals(PathHashAlgorithm.FNV_1A_COMPOSITE_1, resolver.getPathStrategy().getHashAlgorithm());

        // HASHED_PREFIX type with FNV_1A_BASE64
        settings = Settings.builder()
            .put(CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING.getKey(), PathType.HASHED_PREFIX)
            .put(CLUSTER_REMOTE_STORE_PATH_HASH_ALGORITHM_SETTING.getKey(), PathHashAlgorithm.FNV_1A_BASE64)
            .build();
        clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        remoteStoreSettings = new RemoteStoreSettings(settings, clusterSettings);
        resolver = new RemoteStoreCustomMetadataResolver(remoteStoreSettings, () -> Version.V_2_14_0, () -> repositoriesService, settings);
        assertEquals(PathType.HASHED_PREFIX, resolver.getPathStrategy().getType());
        assertEquals(PathHashAlgorithm.FNV_1A_BASE64, resolver.getPathStrategy().getHashAlgorithm());

        // HASHED_PREFIX type with FNV_1A_BASE64
        settings = Settings.builder()
            .put(CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING.getKey(), PathType.HASHED_PREFIX)
            .put(CLUSTER_REMOTE_STORE_PATH_HASH_ALGORITHM_SETTING.getKey(), PathHashAlgorithm.FNV_1A_BASE64)
            .build();
        clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        remoteStoreSettings = new RemoteStoreSettings(settings, clusterSettings);
        resolver = new RemoteStoreCustomMetadataResolver(remoteStoreSettings, () -> Version.V_2_14_0, () -> repositoriesService, settings);
        assertEquals(PathType.HASHED_PREFIX, resolver.getPathStrategy().getType());
        assertEquals(PathHashAlgorithm.FNV_1A_BASE64, resolver.getPathStrategy().getHashAlgorithm());
    }

    public void testGetPathStrategyStrategyWithDynamicUpdate() {

        // Default value
        Settings settings = Settings.builder().build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        RemoteStoreSettings remoteStoreSettings = new RemoteStoreSettings(settings, clusterSettings);
        RemoteStoreCustomMetadataResolver resolver = new RemoteStoreCustomMetadataResolver(
            remoteStoreSettings,
            () -> Version.V_2_14_0,
            () -> repositoriesService,
            settings
        );
        assertEquals(PathType.HASHED_PREFIX, resolver.getPathStrategy().getType());
        assertNotNull(resolver.getPathStrategy().getHashAlgorithm());
        assertEquals(PathHashAlgorithm.FNV_1A_COMPOSITE_1, resolver.getPathStrategy().getHashAlgorithm());

        // Set FIXED with null hash algorithm
        clusterSettings.applySettings(Settings.builder().put(CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING.getKey(), PathType.FIXED).build());
        assertEquals(PathType.FIXED, resolver.getPathStrategy().getType());
        assertNull(resolver.getPathStrategy().getHashAlgorithm());

        // Set HASHED_PREFIX with default hash algorithm
        clusterSettings.applySettings(
            Settings.builder().put(CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING.getKey(), PathType.HASHED_PREFIX).build()
        );
        assertEquals(PathType.HASHED_PREFIX, resolver.getPathStrategy().getType());
        assertEquals(PathHashAlgorithm.FNV_1A_COMPOSITE_1, resolver.getPathStrategy().getHashAlgorithm());

        // Set HASHED_PREFIX with FNV_1A_BASE64 hash algorithm
        clusterSettings.applySettings(
            Settings.builder()
                .put(CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING.getKey(), PathType.HASHED_PREFIX)
                .put(CLUSTER_REMOTE_STORE_PATH_HASH_ALGORITHM_SETTING.getKey(), PathHashAlgorithm.FNV_1A_BASE64)
                .build()
        );
        assertEquals(PathType.HASHED_PREFIX, resolver.getPathStrategy().getType());
        assertEquals(PathHashAlgorithm.FNV_1A_BASE64, resolver.getPathStrategy().getHashAlgorithm());

        // Set HASHED_INFIX with default hash algorithm
        clusterSettings.applySettings(
            Settings.builder().put(CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING.getKey(), PathType.HASHED_INFIX).build()
        );
        assertEquals(PathType.HASHED_INFIX, resolver.getPathStrategy().getType());
        assertEquals(PathHashAlgorithm.FNV_1A_COMPOSITE_1, resolver.getPathStrategy().getHashAlgorithm());

        // Set HASHED_INFIX with FNV_1A_BASE64 hash algorithm
        clusterSettings.applySettings(
            Settings.builder()
                .put(CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING.getKey(), PathType.HASHED_INFIX)
                .put(CLUSTER_REMOTE_STORE_PATH_HASH_ALGORITHM_SETTING.getKey(), PathHashAlgorithm.FNV_1A_BASE64)
                .build()
        );
        assertEquals(PathType.HASHED_INFIX, resolver.getPathStrategy().getType());
        assertEquals(PathHashAlgorithm.FNV_1A_BASE64, resolver.getPathStrategy().getHashAlgorithm());
    }

    public void testTranslogMetadataAllowedTrueWithMinVersionNewer() {
        Settings settings = Settings.builder().put(CLUSTER_REMOTE_STORE_TRANSLOG_METADATA.getKey(), true).build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        RemoteStoreSettings remoteStoreSettings = new RemoteStoreSettings(settings, clusterSettings);
        BlobStoreRepository repositoryMock = mock(BlobStoreRepository.class);
        when(repositoriesService.repository(getRemoteStoreTranslogRepo(settings))).thenReturn(repositoryMock);
        BlobStore blobStoreMock = mock(BlobStore.class);
        when(repositoryMock.blobStore()).thenReturn(blobStoreMock);
        when(blobStoreMock.isBlobMetadataEnabled()).thenReturn(true);
        RemoteStoreCustomMetadataResolver resolver = new RemoteStoreCustomMetadataResolver(
            remoteStoreSettings,
            () -> Version.V_2_15_0,
            () -> repositoriesService,
            settings
        );
        assertTrue(resolver.isTranslogMetadataEnabled());
    }

    public void testTranslogMetadataAllowedFalseWithMinVersionNewer() {
        Settings settings = Settings.builder().put(CLUSTER_REMOTE_STORE_TRANSLOG_METADATA.getKey(), false).build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        RemoteStoreSettings remoteStoreSettings = new RemoteStoreSettings(settings, clusterSettings);
        RemoteStoreCustomMetadataResolver resolver = new RemoteStoreCustomMetadataResolver(
            remoteStoreSettings,
            () -> Version.V_2_15_0,
            () -> repositoriesService,
            settings
        );
        assertFalse(resolver.isTranslogMetadataEnabled());
    }

    public void testTranslogMetadataAllowedMinVersionOlder() {
        Settings settings = Settings.builder().put(CLUSTER_REMOTE_STORE_TRANSLOG_METADATA.getKey(), randomBoolean()).build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        RemoteStoreSettings remoteStoreSettings = new RemoteStoreSettings(settings, clusterSettings);
        RemoteStoreCustomMetadataResolver resolver = new RemoteStoreCustomMetadataResolver(
            remoteStoreSettings,
            () -> Version.V_2_14_0,
            () -> repositoriesService,
            settings
        );
        assertFalse(resolver.isTranslogMetadataEnabled());
    }

    public void testTranslogPathFixedPathSetting() {

        // Default settings
        Settings settings = Settings.builder().build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        RemoteStoreSettings remoteStoreSettings = new RemoteStoreSettings(settings, clusterSettings);
        assertEquals("", remoteStoreSettings.getTranslogPathFixedPrefix());

        // Any other random value
        String randomPrefix = randomAlphaOfLengthBetween(2, 5);
        settings = Settings.builder().put(RemoteStoreSettings.CLUSTER_REMOTE_STORE_TRANSLOG_PATH_PREFIX.getKey(), randomPrefix).build();
        remoteStoreSettings = new RemoteStoreSettings(settings, clusterSettings);
        assertEquals(randomPrefix, remoteStoreSettings.getTranslogPathFixedPrefix());

        // Set any other random value, the setting still points to the old value
        clusterSettings.applySettings(
            Settings.builder()
                .put(RemoteStoreSettings.CLUSTER_REMOTE_STORE_TRANSLOG_PATH_PREFIX.getKey(), randomAlphaOfLengthBetween(2, 5))
                .build()
        );
        assertEquals(randomPrefix, remoteStoreSettings.getTranslogPathFixedPrefix());
    }

    public void testSegmentsPathFixedPathSetting() {

        // Default settings
        Settings settings = Settings.builder().build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        RemoteStoreSettings remoteStoreSettings = new RemoteStoreSettings(settings, clusterSettings);
        assertEquals("", remoteStoreSettings.getSegmentsPathFixedPrefix());

        // Any other random value
        String randomPrefix = randomAlphaOfLengthBetween(2, 5);
        settings = Settings.builder().put(RemoteStoreSettings.CLUSTER_REMOTE_STORE_SEGMENTS_PATH_PREFIX.getKey(), randomPrefix).build();
        remoteStoreSettings = new RemoteStoreSettings(settings, clusterSettings);
        assertEquals(randomPrefix, remoteStoreSettings.getSegmentsPathFixedPrefix());

        // Set any other random value, the setting still points to the old value
        clusterSettings.applySettings(
            Settings.builder()
                .put(RemoteStoreSettings.CLUSTER_REMOTE_STORE_SEGMENTS_PATH_PREFIX.getKey(), randomAlphaOfLengthBetween(2, 5))
                .build()
        );
        assertEquals(randomPrefix, remoteStoreSettings.getSegmentsPathFixedPrefix());

    }
}
