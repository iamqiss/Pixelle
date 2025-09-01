/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.repositories.fs;

import org.density.cluster.metadata.RepositoryMetadata;
import org.density.common.compress.DeflateCompressor;
import org.density.common.settings.ClusterSettings;
import org.density.common.settings.Settings;
import org.density.compress.ZstdCompressor;
import org.density.core.common.unit.ByteSizeUnit;
import org.density.core.compress.CompressorRegistry;
import org.density.core.xcontent.NamedXContentRegistry;
import org.density.env.Environment;
import org.density.indices.recovery.RecoverySettings;
import org.density.repositories.blobstore.BlobStoreTestUtil;
import org.density.test.DensityTestCase;

import java.nio.file.Path;
import java.util.Locale;

public class ReloadableFsRepositoryTests extends DensityTestCase {
    ReloadableFsRepository repository;
    RepositoryMetadata metadata;
    Settings settings;
    Path repo;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        repo = createTempDir();
        settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath())
            .put(Environment.PATH_REPO_SETTING.getKey(), repo.toAbsolutePath())
            .putList(Environment.PATH_DATA_SETTING.getKey(), tmpPaths())
            .put("location", repo)
            .put("compress", false)
            .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
            .put(FsRepository.BASE_PATH_SETTING.getKey(), "my_base_path")
            .build();
        metadata = new RepositoryMetadata("test", "fs", settings);
        repository = new ReloadableFsRepository(
            metadata,
            new Environment(settings, null),
            NamedXContentRegistry.EMPTY,
            BlobStoreTestUtil.mockClusterService(),
            new RecoverySettings(settings, new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS))
        );
    }

    /**
     * Validates that {@link ReloadableFsRepository} supports inplace reloading
     */
    public void testIsReloadable() {
        assertTrue(repository.isReloadable());
    }

    /**
     * Updates repository metadata of an existing repository to enable default compressor
     */
    public void testCompressReload() {
        assertEquals(CompressorRegistry.none(), repository.getCompressor());
        updateCompressionTypeToDefault();
        repository.validateMetadata(metadata);
        repository.reload(metadata);
        assertEquals(CompressorRegistry.defaultCompressor(), repository.getCompressor());
    }

    /**
     * Updates repository metadata of an existing repository to change compressor type from default to Zstd
     */
    public void testCompressionTypeReload() {
        assertEquals(CompressorRegistry.none(), repository.getCompressor());
        updateCompressionTypeToDefault();
        repository = new ReloadableFsRepository(
            metadata,
            new Environment(settings, null),
            NamedXContentRegistry.EMPTY,
            BlobStoreTestUtil.mockClusterService(),
            new RecoverySettings(settings, new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS))
        );
        assertEquals(CompressorRegistry.defaultCompressor(), repository.getCompressor());

        settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath())
            .put(Environment.PATH_REPO_SETTING.getKey(), repo.toAbsolutePath())
            .putList(Environment.PATH_DATA_SETTING.getKey(), tmpPaths())
            .put("location", repo)
            .put("compress", true)
            .put("compression_type", ZstdCompressor.NAME.toLowerCase(Locale.ROOT))
            .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
            .put(FsRepository.BASE_PATH_SETTING.getKey(), "my_base_path")
            .build();
        metadata = new RepositoryMetadata("test", "fs", settings);
        repository.validateMetadata(metadata);
        repository.reload(metadata);
        assertEquals(CompressorRegistry.getCompressor(ZstdCompressor.NAME.toUpperCase(Locale.ROOT)), repository.getCompressor());
    }

    private void updateCompressionTypeToDefault() {
        settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath())
            .put(Environment.PATH_REPO_SETTING.getKey(), repo.toAbsolutePath())
            .putList(Environment.PATH_DATA_SETTING.getKey(), tmpPaths())
            .put("location", repo)
            .put("compress", true)
            .put("compression_type", DeflateCompressor.NAME.toLowerCase(Locale.ROOT))
            .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
            .put(FsRepository.BASE_PATH_SETTING.getKey(), "my_base_path")
            .build();
        metadata = new RepositoryMetadata("test", "fs", settings);
    }
}
