/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.indices;

import org.density.common.settings.ClusterSettings;
import org.density.common.settings.Settings;
import org.density.common.unit.TimeValue;
import org.density.test.DensityTestCase;

import static org.density.indices.RemoteStoreSettings.CLUSTER_REMOTE_TRANSLOG_TRANSFER_TIMEOUT_SETTING;

public class RemoteStoreSettingsDynamicUpdateTests extends DensityTestCase {
    private final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
    private final RemoteStoreSettings remoteStoreSettings = new RemoteStoreSettings(Settings.EMPTY, clusterSettings);

    public void testSegmentMetadataRetention() {
        // Default value
        assertEquals(10, remoteStoreSettings.getMinRemoteSegmentMetadataFiles());

        // Setting value < default (10)
        clusterSettings.applySettings(
            Settings.builder()
                .put(RemoteStoreSettings.CLUSTER_REMOTE_INDEX_SEGMENT_METADATA_RETENTION_MAX_COUNT_SETTING.getKey(), 5)
                .build()
        );
        assertEquals(5, remoteStoreSettings.getMinRemoteSegmentMetadataFiles());

        // Setting min value
        clusterSettings.applySettings(
            Settings.builder()
                .put(RemoteStoreSettings.CLUSTER_REMOTE_INDEX_SEGMENT_METADATA_RETENTION_MAX_COUNT_SETTING.getKey(), -1)
                .build()
        );
        assertEquals(-1, remoteStoreSettings.getMinRemoteSegmentMetadataFiles());

        // Setting value > default (10)
        clusterSettings.applySettings(
            Settings.builder()
                .put(RemoteStoreSettings.CLUSTER_REMOTE_INDEX_SEGMENT_METADATA_RETENTION_MAX_COUNT_SETTING.getKey(), 15)
                .build()
        );
        assertEquals(15, remoteStoreSettings.getMinRemoteSegmentMetadataFiles());

        // Setting value to 0 should fail and retain the existing value
        assertThrows(
            IllegalArgumentException.class,
            () -> clusterSettings.applySettings(
                Settings.builder()
                    .put(RemoteStoreSettings.CLUSTER_REMOTE_INDEX_SEGMENT_METADATA_RETENTION_MAX_COUNT_SETTING.getKey(), 0)
                    .build()
            )
        );
        assertEquals(15, remoteStoreSettings.getMinRemoteSegmentMetadataFiles());

        // Setting value < -1 should fail and retain the existing value
        assertThrows(
            IllegalArgumentException.class,
            () -> clusterSettings.applySettings(
                Settings.builder()
                    .put(RemoteStoreSettings.CLUSTER_REMOTE_INDEX_SEGMENT_METADATA_RETENTION_MAX_COUNT_SETTING.getKey(), -5)
                    .build()
            )
        );
        assertEquals(15, remoteStoreSettings.getMinRemoteSegmentMetadataFiles());
    }

    public void testClusterRemoteTranslogTransferTimeout() {
        // Test default value
        assertEquals(TimeValue.timeValueSeconds(30), remoteStoreSettings.getClusterRemoteTranslogTransferTimeout());

        // Test override with valid value
        clusterSettings.applySettings(Settings.builder().put(CLUSTER_REMOTE_TRANSLOG_TRANSFER_TIMEOUT_SETTING.getKey(), "40s").build());
        assertEquals(TimeValue.timeValueSeconds(40), remoteStoreSettings.getClusterRemoteTranslogTransferTimeout());

        // Test override with value less than minimum
        assertThrows(
            IllegalArgumentException.class,
            () -> clusterSettings.applySettings(
                Settings.builder().put(CLUSTER_REMOTE_TRANSLOG_TRANSFER_TIMEOUT_SETTING.getKey(), "10s").build()
            )
        );
        assertEquals(TimeValue.timeValueSeconds(40), remoteStoreSettings.getClusterRemoteTranslogTransferTimeout());

        // Test override with invalid time value
        assertThrows(
            IllegalArgumentException.class,
            () -> clusterSettings.applySettings(
                Settings.builder().put(CLUSTER_REMOTE_TRANSLOG_TRANSFER_TIMEOUT_SETTING.getKey(), "123").build()
            )
        );
        assertEquals(TimeValue.timeValueSeconds(40), remoteStoreSettings.getClusterRemoteTranslogTransferTimeout());
    }

    public void testMaxRemoteReferencedTranslogFiles() {
        // Test default value
        assertEquals(1000, remoteStoreSettings.getMaxRemoteTranslogReaders());

        // Test override with valid value
        clusterSettings.applySettings(
            Settings.builder().put(RemoteStoreSettings.CLUSTER_REMOTE_MAX_TRANSLOG_READERS.getKey(), "500").build()
        );
        assertEquals(500, remoteStoreSettings.getMaxRemoteTranslogReaders());

        // Test override with value less than minimum
        assertThrows(
            IllegalArgumentException.class,
            () -> clusterSettings.applySettings(
                Settings.builder().put(RemoteStoreSettings.CLUSTER_REMOTE_MAX_TRANSLOG_READERS.getKey(), "99").build()
            )
        );
        assertEquals(500, remoteStoreSettings.getMaxRemoteTranslogReaders());
    }

    public void testDisableMaxRemoteReferencedTranslogFiles() {
        // Test default value
        assertEquals(1000, remoteStoreSettings.getMaxRemoteTranslogReaders());

        // Test override with valid value
        clusterSettings.applySettings(
            Settings.builder().put(RemoteStoreSettings.CLUSTER_REMOTE_MAX_TRANSLOG_READERS.getKey(), "-1").build()
        );
        assertEquals(-1, remoteStoreSettings.getMaxRemoteTranslogReaders());
    }
}
