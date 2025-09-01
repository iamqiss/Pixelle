/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright Density Contributors. See
 * GitHub history for details.
 */

package org.density.cluster.coordination;

import org.density.cluster.block.ClusterBlock;
import org.density.cluster.block.ClusterBlockLevel;
import org.density.common.settings.ClusterSettings;
import org.density.common.settings.Setting;
import org.density.common.settings.Setting.Property;
import org.density.common.settings.Settings;
import org.density.core.rest.RestStatus;

import java.util.EnumSet;

/**
 * Service to block the master node
 *
 * @density.internal
 */
public class NoClusterManagerBlockService {
    public static final int NO_CLUSTER_MANAGER_BLOCK_ID = 2;
    public static final ClusterBlock NO_CLUSTER_MANAGER_BLOCK_WRITES = new ClusterBlock(
        NO_CLUSTER_MANAGER_BLOCK_ID,
        "no cluster-manager",
        true,
        false,
        false,
        RestStatus.SERVICE_UNAVAILABLE,
        EnumSet.of(ClusterBlockLevel.WRITE, ClusterBlockLevel.METADATA_WRITE)
    );
    public static final ClusterBlock NO_CLUSTER_MANAGER_BLOCK_ALL = new ClusterBlock(
        NO_CLUSTER_MANAGER_BLOCK_ID,
        "no cluster-manager",
        true,
        true,
        false,
        RestStatus.SERVICE_UNAVAILABLE,
        ClusterBlockLevel.ALL
    );
    public static final ClusterBlock NO_CLUSTER_MANAGER_BLOCK_METADATA_WRITES = new ClusterBlock(
        NO_CLUSTER_MANAGER_BLOCK_ID,
        "no cluster-manager",
        true,
        false,
        false,
        RestStatus.SERVICE_UNAVAILABLE,
        EnumSet.of(ClusterBlockLevel.METADATA_WRITE)
    );

    public static final Setting<ClusterBlock> NO_MASTER_BLOCK_SETTING = new Setting<>(
        "cluster.no_master_block",
        "metadata_write",
        NoClusterManagerBlockService::parseNoClusterManagerBlock,
        Property.Dynamic,
        Property.NodeScope,
        Property.Deprecated
    );
    // The setting below is going to replace the above.
    // To keep backwards compatibility, the old usage is remained, and it's also used as the fallback for the new usage.
    public static final Setting<ClusterBlock> NO_CLUSTER_MANAGER_BLOCK_SETTING = new Setting<>(
        "cluster.no_cluster_manager_block",
        NO_MASTER_BLOCK_SETTING,
        NoClusterManagerBlockService::parseNoClusterManagerBlock,
        Property.Dynamic,
        Property.NodeScope
    );

    private volatile ClusterBlock noClusterManagerBlock;

    public NoClusterManagerBlockService(Settings settings, ClusterSettings clusterSettings) {
        this.noClusterManagerBlock = NO_CLUSTER_MANAGER_BLOCK_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(NO_CLUSTER_MANAGER_BLOCK_SETTING, this::setNoClusterManagerBlock);
    }

    private static ClusterBlock parseNoClusterManagerBlock(String value) {
        switch (value) {
            case "all":
                return NO_CLUSTER_MANAGER_BLOCK_ALL;
            case "write":
                return NO_CLUSTER_MANAGER_BLOCK_WRITES;
            case "metadata_write":
                return NO_CLUSTER_MANAGER_BLOCK_METADATA_WRITES;
            default:
                throw new IllegalArgumentException(
                    "invalid no-cluster-manager block [" + value + "], must be one of [all, write, metadata_write]"
                );
        }
    }

    public ClusterBlock getNoClusterManagerBlock() {
        return noClusterManagerBlock;
    }

    private void setNoClusterManagerBlock(ClusterBlock noClusterManagerBlock) {
        this.noClusterManagerBlock = noClusterManagerBlock;
    }
}
