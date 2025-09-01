/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.gateway;

import org.density.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.density.common.settings.Setting;
import org.density.common.settings.Settings;
import org.density.test.DensitySingleNodeTestCase;

import static org.density.gateway.ShardsBatchGatewayAllocator.GATEWAY_ALLOCATOR_BATCH_SIZE;
import static org.density.test.hamcrest.DensityAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

public class ShardsBatchGatewayAllocatorTests extends DensitySingleNodeTestCase {
    public void testBatchSizeValueUpdate() {
        Setting<Long> setting1 = GATEWAY_ALLOCATOR_BATCH_SIZE;
        Settings batchSizeSetting = Settings.builder().put(setting1.getKey(), "3000").build();
        try {
            ClusterUpdateSettingsResponse response = client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(batchSizeSetting)
                .execute()
                .actionGet();

            assertAcked(response);
            assertThat(setting1.get(response.getPersistentSettings()), equalTo(3000L));
        } finally {
            // cleanup
            batchSizeSetting = Settings.builder().putNull(setting1.getKey()).build();
            client().admin().cluster().prepareUpdateSettings().setPersistentSettings(batchSizeSetting).execute().actionGet();
        }
    }

    public void testBatchSizeMaxValue() {
        Setting<Long> setting1 = GATEWAY_ALLOCATOR_BATCH_SIZE;
        Settings batchSizeSetting = Settings.builder().put(setting1.getKey(), "11000").build();

        assertThrows(
            "failed to parse value [11000] for setting [" + setting1.getKey() + "], must be <= [10000]",
            IllegalArgumentException.class,
            () -> {
                client().admin().cluster().prepareUpdateSettings().setPersistentSettings(batchSizeSetting).execute().actionGet();
            }
        );
    }

    public void testBatchSizeMinValue() {
        Setting<Long> setting1 = GATEWAY_ALLOCATOR_BATCH_SIZE;
        Settings batchSizeSetting = Settings.builder().put(setting1.getKey(), "0").build();

        assertThrows(
            "failed to parse value [0] for setting [" + setting1.getKey() + "], must be >= [1]",
            IllegalArgumentException.class,
            () -> {
                client().admin().cluster().prepareUpdateSettings().setPersistentSettings(batchSizeSetting).execute().actionGet();
            }
        );
    }
}
