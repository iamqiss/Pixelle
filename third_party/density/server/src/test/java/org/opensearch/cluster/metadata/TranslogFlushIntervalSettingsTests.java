/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.cluster.metadata;

import org.density.common.settings.ClusterSettings;
import org.density.common.settings.Settings;
import org.density.index.IndexSettings;
import org.density.index.compositeindex.CompositeIndexSettings;
import org.density.index.compositeindex.datacube.startree.StarTreeIndexSettings;
import org.density.test.DensityTestCase;

import java.util.Optional;

/**
 * Tests for translog flush interval settings update with and without composite index
 */
public class TranslogFlushIntervalSettingsTests extends DensityTestCase {

    Settings settings = Settings.builder()
        .put(CompositeIndexSettings.COMPOSITE_INDEX_MAX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), "130mb")
        .build();
    ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

    public void testValidSettings() {
        Settings requestSettings = Settings.builder()
            .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), "50mb")
            .put(StarTreeIndexSettings.IS_COMPOSITE_INDEX_SETTING.getKey(), true)
            .put(IndexMetadata.INDEX_APPEND_ONLY_ENABLED_SETTING.getKey(), true)
            .build();

        // This should not throw an exception
        MetadataCreateIndexService.validateTranslogFlushIntervalSettingsForCompositeIndex(requestSettings, clusterSettings);
    }

    public void testDefaultTranslogFlushSetting() {
        Settings requestSettings = Settings.builder()
            .put(StarTreeIndexSettings.IS_COMPOSITE_INDEX_SETTING.getKey(), true)
            .put(IndexMetadata.INDEX_APPEND_ONLY_ENABLED_SETTING.getKey(), true)
            .build();

        // This should not throw an exception
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> MetadataCreateIndexService.validateTranslogFlushIntervalSettingsForCompositeIndex(requestSettings, clusterSettings)
        );
        assertEquals("You can configure 'index.translog.flush_threshold_size' with upto '130mb' for composite index", ex.getMessage());
    }

    public void testMissingCompositeIndexSetting() {
        Settings requestSettings = Settings.builder()
            .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), "50mb")
            .build();

        // This should not throw an exception
        MetadataCreateIndexService.validateTranslogFlushIntervalSettingsForCompositeIndex(requestSettings, clusterSettings);
    }

    public void testNullTranslogFlushSetting() {
        Settings requestSettings = Settings.builder()
            .putNull(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey())
            .put(StarTreeIndexSettings.IS_COMPOSITE_INDEX_SETTING.getKey(), true)
            .put(IndexMetadata.INDEX_APPEND_ONLY_ENABLED_SETTING.getKey(), true)
            .build();

        // This should not throw an exception
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> MetadataCreateIndexService.validateTranslogFlushIntervalSettingsForCompositeIndex(requestSettings, clusterSettings)
        );
        assertEquals("You can configure 'index.translog.flush_threshold_size' with upto '130mb' for composite index", ex.getMessage());
    }

    public void testExceedingMaxFlushSize() {
        Settings requestSettings = Settings.builder()
            .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), "150mb")
            .put(StarTreeIndexSettings.IS_COMPOSITE_INDEX_SETTING.getKey(), true)
            .put(IndexMetadata.INDEX_APPEND_ONLY_ENABLED_SETTING.getKey(), true)
            .build();

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> MetadataCreateIndexService.validateTranslogFlushIntervalSettingsForCompositeIndex(requestSettings, clusterSettings)
        );
        assertEquals("You can configure 'index.translog.flush_threshold_size' with upto '130mb' for composite index", ex.getMessage());
    }

    public void testEqualToMaxFlushSize() {
        Settings requestSettings = Settings.builder()
            .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), "100mb")
            .put(StarTreeIndexSettings.IS_COMPOSITE_INDEX_SETTING.getKey(), true)
            .put(IndexMetadata.INDEX_APPEND_ONLY_ENABLED_SETTING.getKey(), true)
            .build();

        // This should not throw an exception
        MetadataCreateIndexService.validateTranslogFlushIntervalSettingsForCompositeIndex(requestSettings, clusterSettings);
    }

    public void testUpdateIndexThresholdFlushSize() {
        Settings requestSettings = Settings.builder()
            .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), "100mb")
            .build();

        Settings indexSettings = Settings.builder()
            .put(StarTreeIndexSettings.IS_COMPOSITE_INDEX_SETTING.getKey(), true)
            .put(IndexMetadata.INDEX_APPEND_ONLY_ENABLED_SETTING.getKey(), true)
            .build();

        // This should not throw an exception
        assertTrue(
            MetadataCreateIndexService.validateTranslogFlushIntervalSettingsForCompositeIndex(
                requestSettings,
                clusterSettings,
                indexSettings
            ).isEmpty()
        );
    }

    public void testUpdateFlushSizeAboveThresholdWithCompositeIndex() {
        Settings requestSettings = Settings.builder()
            .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), "131mb")
            .build();

        Settings indexSettings = Settings.builder()
            .put(StarTreeIndexSettings.IS_COMPOSITE_INDEX_SETTING.getKey(), true)
            .put(IndexMetadata.INDEX_APPEND_ONLY_ENABLED_SETTING.getKey(), true)
            .build();

        Optional<String> err = MetadataCreateIndexService.validateTranslogFlushIntervalSettingsForCompositeIndex(
            requestSettings,
            clusterSettings,
            indexSettings
        );
        assertTrue(err.isPresent());
        assertEquals("You can configure 'index.translog.flush_threshold_size' with upto '130mb' for composite index", err.get());
    }

    public void testUpdateFlushSizeAboveThresholdWithoutCompositeIndex() {
        Settings requestSettings = Settings.builder()
            .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), "131mb")
            .build();

        Settings indexSettings = Settings.builder().build();

        // This should not throw an exception
        assertTrue(
            MetadataCreateIndexService.validateTranslogFlushIntervalSettingsForCompositeIndex(
                requestSettings,
                clusterSettings,
                indexSettings
            ).isEmpty()
        );
    }
}
